// Manual Apply Loop.
//
// Demonstrates:
// - the StateMachine interface committed entries are meant to reach
// - RaftCore not applying anything for you yet: commitIndex advances while
//   lastApplied stays at 0
// - a hand-written apply loop: entries lastApplied+1 through commitIndex,
//   in order, each recorded with setLastApplied()
// - running that loop every round, so each node's state machine follows
//   its own commit index

#include <support/framework.h>

using namespace RaftCore;

namespace {

// PersistentState: the durable currentTerm and votedFor.
class MemoryPersistentState : public PersistentState {
  public:
    Status setCurrentTerm(Term term) override {
        term_ = term;
        return Status::OK;
    }
    Term currentTerm() const override {
        return term_;
    }

    Status setVotedFor(std::optional<NodeId> candidate) override {
        votedFor_ = std::move(candidate);
        return Status::OK;
    }
    std::optional<NodeId> votedFor() const override {
        return votedFor_;
    }

  private:
    Term term_ = 0;
    std::optional<NodeId> votedFor_;
};

// Storage: the log. Whole entries are kept, payload included, so the
// example can read back what was replicated.
class MemoryStorage : public Storage {
  public:
    Status append(Term term, Vector<std::uint8_t> payload, LogIndex& outIndex) override {
        LogEntry entry;
        entry.term = term;
        entry.payload = std::move(payload);
        entries_.push_back(std::move(entry));
        outIndex = entries_.size();
        return Status::OK;
    }
    Status entryAt(LogIndex index, LogEntry& outEntry) const override {
        if (index == kNoIndex || index > entries_.size())
            return Status::NOT_FOUND;
        outEntry = entries_[index - 1];
        return Status::OK;
    }
    Status range(LogIndex fromIndex, LogIndex toIndex,
                 Vector<LogEntry>& outEntries) const override {
        if (fromIndex == kNoIndex || fromIndex > toIndex || toIndex > entries_.size())
            return Status::NOT_FOUND;
        for (LogIndex i = fromIndex; i <= toIndex; ++i)
            outEntries.push_back(entries_[i - 1]);
        return Status::OK;
    }
    Status truncateFrom(LogIndex index) override {
        if (index != kNoIndex && index <= entries_.size()) {
            while (entries_.size() > index - 1)
                entries_.pop_back();
        }
        return Status::OK;
    }
    LogIndex lastIndex() const override {
        return entries_.size();
    }
    Term lastTerm() const override {
        return entries_.size() == 0 ? kNoTerm : entries_[entries_.size() - 1].term;
    }

  private:
    Vector<LogEntry> entries_;
};

// RandomSource: picks each election timeout. Returning the lower bound
// makes it exact -- and each node below gets its own bound.
class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// Transport for one cluster: queues each RPC, delivers on deliverAll(), and
// can take a node "down". While a node is down, RPCs to it and RPCs it
// sends are silently lost -- the sender's callback is dropped without being
// called, exactly like a real network that never answers. RaftCore's own
// timeouts do the noticing.
class Network : public Transport {
  public:
    void attach(const NodeId& id, RaftNode& node) {
        nodes_[id] = &node;
    }

    void setDown(const NodeId& id, bool down) {
        down_[id] = down;
    }

    Status sendRequestVote(const NodeId& target, RequestVoteArgs args,
                           MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;
        if (isDown(target) || isDown(args.candidateId))
            return Status::OK; // Lost in transit.

        // The job's data lives behind one unique_ptr: FunctionPro requires the
        // stored callable to be nothrow-move-constructible, and a lone
        // unique_ptr always is.
        std::unique_ptr<VoteJob> job(
            new VoteJob{nodes_[target], std::move(args), std::move(onReply)});
        queue_.push_back(MoveOnlyFunction<void()>([job = std::move(job)]() {
            RequestVoteReply reply;
            if (job->node->handleRequestVote(job->args, reply) != Status::OK)
                return;
            job->onReply(reply);
        }));
        return Status::OK;
    }

    Status sendAppendEntries(const NodeId& target, AppendEntriesArgs args,
                             MoveOnlyFunction<void(AppendEntriesReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;
        if (isDown(target) || isDown(args.leaderId))
            return Status::OK; // Lost in transit.

        std::unique_ptr<AppendJob> job(
            new AppendJob{nodes_[target], std::move(args), std::move(onReply)});
        queue_.push_back(MoveOnlyFunction<void()>([job = std::move(job)]() {
            AppendEntriesReply reply;
            if (job->node->handleAppendEntries(std::move(job->args), reply) != Status::OK)
                return;
            job->onReply(reply);
        }));
        return Status::OK;
    }

    // Delivers everything queued, including RPCs queued along the way.
    void deliverAll() {
        while (next_ < queue_.size()) {
            MoveOnlyFunction<void()> job = std::move(queue_[next_]);
            ++next_;
            job();
        }
    }

  private:
    bool isDown(const NodeId& id) {
        return down_.contains(id) && down_[id];
    }

    // What a queued delivery needs, kept together behind a single pointer.
    struct VoteJob {
        RaftNode* node;
        RequestVoteArgs args;
        MoveOnlyFunction<void(RequestVoteReply)> onReply;
    };
    struct AppendJob {
        RaftNode* node;
        AppendEntriesArgs args;
        MoveOnlyFunction<void(AppendEntriesReply)> onReply;
    };

    HashMap<NodeId, RaftNode*> nodes_;
    HashMap<NodeId, bool> down_;
    Vector<MoveOnlyFunction<void()>> queue_;
    std::size_t next_ = 0;
};

// One cluster member: its own state, storage, and RaftNode on the shared
// Network. `timeout` is the election timeout in ticks; heartbeat is 2 ticks.
struct Member {
    Member(const NodeId& id, Vector<NodeId> peers, Network& network, RandomSource& random,
           int timeout)
        : state(persistent, storage, id),
          raft(state, network, random, std::move(peers), timeout, timeout, 2) {
        network.attach(id, raft);
    }

    MemoryPersistentState persistent;
    MemoryStorage storage;
    NodeState state;
    RaftNode raft;
};

Vector<NodeId> peersOf(const char* a, const char* b) {
    Vector<NodeId> peers;
    peers.push_back(NodeId(a));
    peers.push_back(NodeId(b));
    return peers;
}

const char* roleName(Role role) {
    switch (role) {
    case Role::Follower:
        return "Follower";
    case Role::Candidate:
        return "Candidate";
    case Role::Leader:
        return "Leader";
    }
    return "?";
}

// Text -> bytes, for entry payloads.
Vector<std::uint8_t> toBytes(const std::string& text) {
    Vector<std::uint8_t> bytes;
    for (char c : text)
        bytes.push_back(static_cast<std::uint8_t>(c));
    return bytes;
}

// Bytes -> text, for entry payloads.
std::string toText(const Vector<std::uint8_t>& bytes) {
    std::string text;
    for (std::size_t i = 0; i < bytes.size(); ++i)
        text.push_back(static_cast<char>(bytes[i]));
    return text;
}

// Advances every node by one tick, in lockstep.
template <std::size_t N> void tickAll(Member* const (&members)[N]) {
    for (Member* m : members)
        (void)m->raft.tick();
}

template <std::size_t N> void printCluster(Member* const (&members)[N]) {
    for (const Member* m : members) {
        std::cout << m->state.selfId() << " : " << roleName(m->state.role()) << ", term "
                  << m->state.currentTerm() << ", voted for " << m->state.votedFor().value_or("-")
                  << ", log " << m->state.log().lastIndex() << ", commit " << m->state.commitIndex()
                  << "\n";
    }
    std::cout << "\n";
}

// A tiny replicated key-value store: the state machine. Commands are the
// text "set <key>=<value>". Every node has its own copy, and RaftCore's
// job is to make sure they all see the same commands in the same order.
class KeyValueStore : public StateMachine {
  public:
    void apply(LogIndex index, const Vector<std::uint8_t>& payload) override {
        applied.push_back(index);

        // A new Leader's no-op entry has an empty payload: it's a log entry
        // like any other, so it's applied too -- a state machine just has
        // nothing to do for it.
        if (payload.size() == 0)
            return;

        const std::string text = toText(payload);
        if (text.rfind("set ", 0) != 0)
            return;
        const std::size_t equals = text.find('=');
        if (equals == std::string::npos)
            return;
        data[text.substr(4, equals - 4)] = text.substr(equals + 1);
    }

    std::string get(const std::string& key) {
        return data.contains(key) ? data[key] : "-";
    }

    Vector<LogIndex> applied;
    HashMap<std::string, std::string> data;
};

// Applies every committed-but-unapplied entry to `machine`, oldest first,
// recording progress in `state` as it goes. Returns the first storage
// failure, or OK. `count` is how many entries were applied by this call.
//
// This is the whole contract a Raft node owes its state machine: in
// order, each index once, and only what's committed. Recording each
// index (setLastApplied) right after applying it is what makes a second
// call -- or a call after a restart -- pick up exactly where this one
// stopped instead of applying anything twice.
Status applyCommitted(NodeState& state, StateMachine& machine, std::size_t& count) {
    count = 0;
    while (state.lastApplied() < state.commitIndex()) {
        const LogIndex next = state.lastApplied() + 1;
        LogEntry entry;
        Status status = state.log().entryAt(next, entry);
        if (status != Status::OK)
            return status;

        machine.apply(next, entry.payload);
        state.setLastApplied(next);
        ++count;
    }
    return Status::OK;
}

} // namespace

static void run_examples() {
    // StateMachine has one method, apply(index, payload). Three nodes,
    // three independent stores, one per node.
    setTitle("Implement a State Machine");

    Network network;
    FixedRandom random;
    Member node1("node-1", peersOf("node-2", "node-3"), network, random, 3);
    Member node2("node-2", peersOf("node-1", "node-3"), network, random, 5);
    Member node3("node-3", peersOf("node-1", "node-2"), network, random, 7);
    Member* const members[] = {&node1, &node2, &node3};
    KeyValueStore stores[3];

    std::cout << "3 nodes, 3 empty key-value stores\n\n";

    // Elect node-1 and replicate three commands, as in
    // integration/three_node_cluster.cpp. Every node ends up with the
    // entries in its log and its commit index advanced -- but nothing
    // has been applied: lastApplied is still 0 and the stores are empty.
    // RaftCore records what's committed; getting it into your state
    // machine is up to you.
    setTitle("Commit Some Commands");

    for (int i = 0; i < 3; ++i)
        tickAll(members);
    network.deliverAll();

    LogIndex last = kNoIndex;
    for (const char* command : {"set a=1", "set b=2", "set c=3"})
        (void)node1.storage.append(node1.state.currentTerm(), toBytes(command), last);

    for (int i = 0; i < 30; ++i) {
        bool done = true;
        for (const Member* m : members)
            done = done && m->state.commitIndex() >= last;
        if (done)
            break;
        tickAll(members);
        network.deliverAll();
    }

    for (int i = 0; i < 3; ++i) {
        std::cout << members[i]->state.selfId() << " : commit " << members[i]->state.commitIndex()
                  << ", lastApplied " << members[i]->state.lastApplied()
                  << ", a=" << stores[i].get("a") << "\n";
    }
    std::cout << "\n";

    // Run the apply loop on every node. Each store gets index 1 (the
    // no-op) through index 4 (set c=3), in order.
    setTitle("Apply by Hand");

    for (int i = 0; i < 3; ++i) {
        std::size_t count = 0;
        Status status = applyCommitted(members[i]->state, stores[i], count);
        std::cout << members[i]->state.selfId() << " : applied " << count << " ("
                  << (status == Status::OK ? "OK" : "error") << "), indices";
        for (std::size_t k = 0; k < stores[i].applied.size(); ++k)
            std::cout << " " << stores[i].applied[k];
        std::cout << ", a=" << stores[i].get("a") << " b=" << stores[i].get("b")
                  << " c=" << stores[i].get("c") << "\n";
    }

    // Calling it again applies nothing: lastApplied already caught up to
    // commitIndex, which is exactly the once-only guarantee.
    std::size_t again = 0;
    (void)applyCommitted(node1.state, stores[0], again);
    std::cout << "\nsecond call on node-1 applied : " << again << "\n\n"; // (0)

    // In a real application the loop runs wherever time advances -- here,
    // once per round, right after ticking and delivering. A new command
    // on the Leader reaches its store when it commits; followers apply it
    // a heartbeat later, when they learn the new commit index.
    setTitle("Apply Every Round");

    (void)node1.storage.append(node1.state.currentTerm(), toBytes("set a=9"), last);

    int rounds = 0;
    for (; rounds < 30; ++rounds) {
        tickAll(members);
        network.deliverAll();
        for (int i = 0; i < 3; ++i) {
            std::size_t count = 0;
            (void)applyCommitted(members[i]->state, stores[i], count);
        }
        if (stores[0].get("a") == "9" && stores[1].get("a") == "9" && stores[2].get("a") == "9")
            break;
    }
    std::cout << "rounds until every store had a=9 : " << rounds + 1 << "\n";
    for (int i = 0; i < 3; ++i) {
        std::cout << members[i]->state.selfId() << " : lastApplied "
                  << members[i]->state.lastApplied() << ", a=" << stores[i].get("a") << "\n";
    }
}

REGISTER_EXAMPLE_SUITE();
