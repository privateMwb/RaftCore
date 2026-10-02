// Three-Node Cluster.
//
// Demonstrates, in one session:
// - electing a Leader
// - replicating several entries and committing them across the cluster
// - checking that every node ends up with an identical log
//
// RaftCore has no propose() call yet, so entries are appended to the
// Leader's Storage directly -- see advanced/propose_by_hand.cpp.

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

// Bytes <-> text, for entry payloads.
Vector<std::uint8_t> toBytes(const std::string& text) {
    Vector<std::uint8_t> bytes;
    for (char c : text)
        bytes.push_back(static_cast<std::uint8_t>(c));
    return bytes;
}

std::string toText(const Vector<std::uint8_t>& bytes) {
    std::string text;
    for (std::size_t i = 0; i < bytes.size(); ++i)
        text.push_back(static_cast<char>(bytes[i]));
    return text;
}

// One line per log entry: index, term, payload ("(no-op)" when empty --
// that's the entry a new Leader appends when it's elected).
void printLog(const NodeId& id, const Storage& log) {
    std::cout << id << " log :";
    if (log.lastIndex() == 0)
        std::cout << " (empty)";
    for (LogIndex i = 1; i <= log.lastIndex(); ++i) {
        LogEntry entry;
        (void)log.entryAt(i, entry);
        std::cout << " [" << i << ": term " << entry.term << ", "
                  << (entry.payload.size() == 0 ? "(no-op)" : "\"" + toText(entry.payload) + "\"")
                  << "]";
    }
    std::cout << "\n";
}

// Advances every node by one tick, in lockstep.
void tickAll(Member* const (&members)[3]) {
    for (Member* m : members)
        (void)m->raft.tick();
}

void printCluster(Member* const (&members)[3]) {
    for (const Member* m : members) {
        std::cout << m->state.selfId() << " : " << roleName(m->state.role()) << ", term "
                  << m->state.currentTerm() << ", voted for " << m->state.votedFor().value_or("-")
                  << ", log " << m->state.log().lastIndex() << ", commit " << m->state.commitIndex()
                  << "\n";
    }
    std::cout << "\n";
}

} // namespace

static void run_examples() {
    // Three nodes with different election timeouts (3, 5, and 7 ticks) on
    // one Network, so node-1 is guaranteed to time out first.
    setTitle("Build the Cluster");

    Network network;
    FixedRandom random;
    Member node1("node-1", peersOf("node-2", "node-3"), network, random, 3);
    Member node2("node-2", peersOf("node-1", "node-3"), network, random, 5);
    Member node3("node-3", peersOf("node-1", "node-2"), network, random, 7);
    Member* const members[] = {&node1, &node2, &node3};

    printCluster(members);

    // node-1 times out, wins both votes, and becomes Leader; its first
    // heartbeat carries the no-op entry every new Leader appends, and the
    // followers accept it.
    setTitle("Elect a Leader");

    for (int i = 0; i < 3; ++i)
        tickAll(members);
    network.deliverAll();
    printCluster(members);

    // Three commands go into the Leader's log. Nothing is sent yet; the
    // next heartbeat carries all of them at once, the followers append them
    // and acknowledge, and once a majority has an entry the Leader commits
    // it. Followers learn the new commit index one heartbeat later, so
    // keep ticking until every node has committed everything.
    setTitle("Replicate Three Entries");

    LogIndex last = kNoIndex;
    for (const char* command : {"set a=1", "set b=2", "set c=3"})
        (void)node1.storage.append(node1.state.currentTerm(), toBytes(command), last);

    int rounds = 0;
    while (rounds < 30 && (node1.state.commitIndex() < last || node2.state.commitIndex() < last ||
                           node3.state.commitIndex() < last)) {
        tickAll(members);
        network.deliverAll();
        ++rounds;
    }
    std::cout << "rounds until everyone committed : " << rounds << "\n\n";
    printCluster(members);

    // The Log Matching property, checked directly: same length, and every
    // entry has the same term and payload on every node.
    setTitle("Compare the Logs");

    for (const Member* m : members)
        printLog(m->state.selfId(), m->state.log());

    bool identical = true;
    for (const Member* m : {&node2, &node3}) {
        if (m->state.log().lastIndex() != node1.state.log().lastIndex())
            identical = false;
        for (LogIndex i = 1; i <= node1.state.log().lastIndex(); ++i) {
            LogEntry a;
            LogEntry b;
            (void)node1.state.log().entryAt(i, a);
            (void)m->state.log().entryAt(i, b);
            if (a.term != b.term || toText(a.payload) != toText(b.payload))
                identical = false;
        }
    }
    std::cout << "\nlogs identical : " << (identical ? "yes" : "no") << "\n"; // (yes)
}

REGISTER_EXAMPLE_SUITE();
