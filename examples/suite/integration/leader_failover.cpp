// Leader Failover.
//
// Demonstrates:
// - a Leader being cut off from the cluster
// - a follower timing out, winning a vote from the other survivor, and
//   becoming the new Leader in a higher term
// - the new Leader committing with only two of three nodes reachable
// - the old Leader rejoining, discovering it is stale, stepping down, and
//   catching up

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
    // Same three-node cluster as three_node_cluster.cpp, with node-1
    // elected Leader and one entry committed everywhere.
    setTitle("Start With a Working Cluster");

    Network network;
    FixedRandom random;
    Member node1("node-1", peersOf("node-2", "node-3"), network, random, 3);
    Member node2("node-2", peersOf("node-1", "node-3"), network, random, 5);
    Member node3("node-3", peersOf("node-1", "node-2"), network, random, 7);
    Member* const members[] = {&node1, &node2, &node3};

    for (int i = 0; i < 3; ++i)
        tickAll(members);
    network.deliverAll();

    LogIndex before = kNoIndex;
    (void)node1.storage.append(node1.state.currentTerm(), toBytes("before failover"), before);
    for (int i = 0;
         i < 30 && (node2.state.commitIndex() < before || node3.state.commitIndex() < before);
         ++i) {
        tickAll(members);
        network.deliverAll();
    }
    printCluster(members);

    // node-1 stops sending and receiving. It has no way to know: as far as
    // it can tell it's still the Leader, and keeps heartbeating into the
    // void. The followers just stop hearing from it. The one with the
    // shortest timeout (node-2, 5 ticks) gives up first, becomes a
    // Candidate in term 2, and node-3 -- which hasn't voted in term 2 and
    // whose log is as up to date -- grants the vote. Two of three is a
    // majority.
    setTitle("Cut Off the Leader");

    network.setDown("node-1", true);

    Member* leader = nullptr;
    int rounds = 0;
    while (leader == nullptr && rounds < 30) {
        tickAll(members);
        network.deliverAll();
        ++rounds;
        for (Member* m : {&node2, &node3}) {
            if (m->state.role() == Role::Leader)
                leader = m;
        }
    }
    std::cout << "rounds until a new Leader : " << rounds << "\n";
    if (leader != nullptr)
        std::cout << "new Leader                : " << leader->state.selfId() << "\n\n";
    printCluster(members); // (node-1 still calls itself Leader, in the old term)

    if (leader == nullptr)
        return;
    Member* other = (leader == &node2) ? &node3 : &node2;

    // With node-1 unreachable, the new Leader has one follower -- and one
    // acknowledgement plus its own copy is a majority of three, so entries
    // still commit.
    setTitle("Commit With Two of Three");

    LogIndex after = kNoIndex;
    (void)leader->storage.append(leader->state.currentTerm(), toBytes("after failover"), after);
    for (int i = 0;
         i < 30 && (leader->state.commitIndex() < after || other->state.commitIndex() < after);
         ++i) {
        tickAll(members);
        network.deliverAll();
    }
    printCluster(members); // (node-1's log is one entry short and behind on commits)

    // Reconnect node-1. Its next heartbeat reaches followers who reject it:
    // it carries term 1 and they're in term 2. That rejection carries the
    // higher term back, so node-1 steps down. The new Leader's heartbeats
    // then bring its log up to date.
    setTitle("The Old Leader Rejoins");

    network.setDown("node-1", false);
    for (int i = 0; i < 30 && (node1.state.role() != Role::Follower ||
                               node1.state.commitIndex() < leader->state.commitIndex());
         ++i) {
        tickAll(members);
        network.deliverAll();
    }
    printCluster(members); // (all in term 2, logs and commits equal)

    for (const Member* m : members)
        printLog(m->state.selfId(), m->state.log());
}

REGISTER_EXAMPLE_SUITE();
