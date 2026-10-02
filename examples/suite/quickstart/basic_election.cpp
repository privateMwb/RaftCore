// Basic Election.
//
// Demonstrates:
// - a three-node cluster whose nodes have different election timeouts
// - a Transport that queues RPCs, so the example controls delivery
// - one node timing out, winning a majority, and becoming Leader
// - the Leader's heartbeats keeping the followers from timing out

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

// Storage: the log. Kept as a list of terms -- payloads don't matter here.
class MemoryStorage : public Storage {
  public:
    Status append(Term term, Vector<std::uint8_t>, LogIndex& outIndex) override {
        terms_.push_back(term);
        outIndex = terms_.size();
        return Status::OK;
    }
    Status entryAt(LogIndex index, LogEntry& outEntry) const override {
        if (index == kNoIndex || index > terms_.size())
            return Status::NOT_FOUND;
        outEntry.term = terms_[index - 1];
        return Status::OK;
    }
    Status range(LogIndex fromIndex, LogIndex toIndex,
                 Vector<LogEntry>& outEntries) const override {
        if (fromIndex == kNoIndex || fromIndex > toIndex || toIndex > terms_.size())
            return Status::NOT_FOUND;
        for (LogIndex i = fromIndex; i <= toIndex; ++i) {
            LogEntry entry;
            entry.term = terms_[i - 1];
            outEntries.push_back(std::move(entry));
        }
        return Status::OK;
    }
    Status truncateFrom(LogIndex index) override {
        if (index != kNoIndex && index <= terms_.size()) {
            while (terms_.size() > index - 1)
                terms_.pop_back();
        }
        return Status::OK;
    }
    LogIndex lastIndex() const override {
        return terms_.size();
    }
    Term lastTerm() const override {
        return terms_.size() == 0 ? kNoTerm : terms_[terms_.size() - 1];
    }

  private:
    Vector<Term> terms_;
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

static void run_examples() {
    // Three nodes share one Network. Their election timeouts differ (3, 5,
    // and 7 ticks) so node-1 is guaranteed to time out first -- in a real
    // cluster the RandomSource spreads the timeouts out for you.
    setTitle("Build a Three-Node Cluster");

    Network network;
    FixedRandom random;
    Member node1("node-1", peersOf("node-2", "node-3"), network, random, 3);
    Member node2("node-2", peersOf("node-1", "node-3"), network, random, 5);
    Member node3("node-3", peersOf("node-1", "node-2"), network, random, 7);
    const Members members = {&node1, &node2, &node3};

    printCluster(members);

    // Time only moves when tick() is called, so tick all three together.
    // After three ticks node-1 has heard nothing from a leader: it bumps
    // its term, votes for itself, and sends RequestVote to both peers. The
    // Network queued those, so nobody has seen them yet.
    setTitle("Let One Node Time Out");

    for (int i = 0; i < 3; ++i)
        tickAll(members);

    printCluster(members);
    std::cout << "RPCs in flight : " << network.pending() << "\n\n"; // (2 RequestVotes)

    // Deliver them. node-2 and node-3 each haven't voted this term and
    // node-1's log is at least as up to date as theirs, so both grant. On
    // the first granted reply node-1 has 2 of 3 votes -- a majority -- and
    // becomes Leader; it appends a no-op entry and sends its first
    // AppendEntries, which deliverAll() also delivers.
    setTitle("Deliver the Votes");

    network.deliverAll();
    printCluster(members);

    // The Leader now heartbeats every 2 ticks. Each heartbeat resets a
    // follower's election timer, so neither node-2 (5 ticks) nor node-3
    // (7 ticks) ever times out -- ten rounds later nothing has changed
    // except that the followers have learned the no-op is committed.
    setTitle("Heartbeats Keep the Peace");

    for (int i = 0; i < 10; ++i) {
        tickAll(members);
        network.deliverAll();
    }
    printCluster(members);
}

REGISTER_EXAMPLE_SUITE();
