// Basic Replication.
//
// Demonstrates:
// - a Leader replicating a log entry to its followers on the heartbeat
// - the Leader committing once a majority has the entry
// - followers learning the commit one heartbeat later
// - reading the replicated entry back out of a follower's log
//
// RaftCore has no propose() call yet, so the example appends the entry to
// the Leader's Storage directly -- see advanced/propose_by_hand.cpp.

#include <support/framework.h>

#include <array>
#include <string>

using namespace RaftCore;

namespace {

// RaftCore talks to four interfaces: PersistentState, Storage, Transport,
// and RandomSource. These are the smallest in-memory implementations of
// each. Only the Transport is interesting: it queues every RPC instead of
// delivering it, so the example decides exactly when messages arrive.

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

// Transport: instead of sending anything, queues a job that will hand the
// RPC to the target node and pass its reply back to the sender. Nothing
// moves until deliverAll() -- which also drains anything new that the
// deliveries themselves send (a granted vote makes a Leader, which sends
// AppendEntries, and so on).
class Network : public Transport {
  public:
    void attach(const NodeId& id, RaftNode& node) {
        nodes_[id] = &node;
    }

    Status sendRequestVote(const NodeId& target, RequestVoteArgs args,
                           MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        // The job's data lives behind one unique_ptr: FunctionPro requires the
        // stored callable to be nothrow-move-constructible, and a lone
        // unique_ptr always is.
        std::unique_ptr<VoteJob> job(
            new VoteJob{nodes_[target], target, std::move(args), std::move(onReply)});
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
        std::unique_ptr<AppendJob> job(
            new AppendJob{nodes_[target], target, std::move(args), std::move(onReply)});
        queue_.push_back(MoveOnlyFunction<void()>([job = std::move(job)]() {
            AppendEntriesReply reply;
            if (job->node->handleAppendEntries(std::move(job->args), reply) != Status::OK)
                return;
            job->onReply(reply);
        }));
        return Status::OK;
    }

    // How many RPCs are waiting to be delivered.
    std::size_t pending() const {
        return queue_.size() - next_;
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
    // What a queued delivery needs, kept together behind a single pointer.
    struct VoteJob {
        RaftNode* node;
        NodeId target;
        RequestVoteArgs args;
        MoveOnlyFunction<void(RequestVoteReply)> onReply;
    };
    struct AppendJob {
        RaftNode* node;
        NodeId target;
        AppendEntriesArgs args;
        MoveOnlyFunction<void(AppendEntriesReply)> onReply;
    };

    HashMap<NodeId, RaftNode*> nodes_;
    Vector<MoveOnlyFunction<void()>> queue_;
    std::size_t next_ = 0;
};

// One cluster member: its own state, storage, and RaftNode, all sharing the
// cluster's Network. `timeout` is the election timeout in ticks; the
// heartbeat interval, once Leader, is 2 ticks.
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

using Members = std::array<Member*, 3>;

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

// Advances every node by one tick, in lockstep.
void tickAll(const Members& members) {
    for (Member* m : members)
        (void)m->raft.tick();
}

void printCluster(const Members& members) {
    for (Member* m : members) {
        std::cout << m->state.selfId() << " : " << roleName(m->state.role()) << ", term "
                  << m->state.currentTerm() << ", voted for " << m->state.votedFor().value_or("-")
                  << ", log " << m->state.log().lastIndex() << ", commit " << m->state.commitIndex()
                  << "\n";
    }
    std::cout << "\n";
}

} // namespace

// Bytes <-> text, for the entry payload.
static Vector<std::uint8_t> toBytes(const std::string& text) {
    Vector<std::uint8_t> bytes;
    for (char c : text)
        bytes.push_back(static_cast<std::uint8_t>(c));
    return bytes;
}

static std::string toText(const Vector<std::uint8_t>& bytes) {
    std::string text;
    for (std::size_t i = 0; i < bytes.size(); ++i)
        text.push_back(static_cast<char>(bytes[i]));
    return text;
}

static void run_examples() {
    // Same three-node cluster as basic_election.cpp; node-1 times out
    // first, wins, and becomes Leader. Its log already holds one entry:
    // the no-op a new Leader appends, which is committed once a majority
    // has it.
    setTitle("Elect a Leader");

    Network network;
    FixedRandom random;
    Member node1("node-1", peersOf("node-2", "node-3"), network, random, 3);
    Member node2("node-2", peersOf("node-1", "node-3"), network, random, 5);
    Member node3("node-3", peersOf("node-1", "node-2"), network, random, 7);
    const Members members = {&node1, &node2, &node3};

    for (int i = 0; i < 3; ++i)
        tickAll(members);
    network.deliverAll();
    printCluster(members);

    // Append an entry to the Leader's own log, tagged with the current
    // term. Nothing is sent yet, so only node-1 has it.
    setTitle("Append an Entry on the Leader");

    LogIndex index = kNoIndex;
    Status s = node1.storage.append(node1.state.currentTerm(), toBytes("set x=1"), index);
    std::cout << "append status : " << static_cast<int>(s) << "\n"; // (OK)
    std::cout << "entry index   : " << index << "\n\n";

    printCluster(members);

    // The Leader sends new entries on its next heartbeat. Tick until the
    // Leader's commit index reaches the new entry: both followers have
    // appended it by then and their acknowledgements gave the Leader a
    // majority -- but the followers haven't been told it's committed yet.
    setTitle("Replicate on the Heartbeat");

    int rounds = 0;
    while (node1.state.commitIndex() < index && rounds < 20) {
        tickAll(members);
        network.deliverAll();
        ++rounds;
    }
    std::cout << "rounds : " << rounds << "\n\n";
    printCluster(members);

    // The commit index rides along on the next heartbeat, so the
    // followers catch up one heartbeat later.
    setTitle("Followers Learn the Commit");

    rounds = 0;
    while ((node2.state.commitIndex() < index || node3.state.commitIndex() < index) &&
           rounds < 20) {
        tickAll(members);
        network.deliverAll();
        ++rounds;
    }
    std::cout << "rounds : " << rounds << "\n\n";
    printCluster(members);

    // Read the entry back out of each follower's log.
    setTitle("Read It Back");

    for (Member* m : {&node2, &node3}) {
        LogEntry entry;
        Status r = m->state.log().entryAt(index, entry);
        std::cout << m->state.selfId() << " status : " << static_cast<int>(r) // (OK)
                  << ", term " << entry.term << ", payload \"" << toText(entry.payload) << "\"\n";
    }
}

REGISTER_EXAMPLE_SUITE();
