// Identical Timeouts.
//
// Demonstrates:
// - three nodes with the same election timeout all becoming Candidates
//   on the same tick, splitting the vote, and repeating forever
// - the fix: timeouts that differ from node to node
//
// The trap is easy to fall into in tests and simulations, where a
// deterministic RandomSource makes "random" timeouts identical. In
// production, SystemRandomSource picks each timeout from a range, which is
// what breaks the tie.

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

// Storage: the log, kept as a list of terms -- payloads don't matter here.
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
// makes it exact.
class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// Transport that routes RPCs between the nodes of one cluster (see
// message_routing.cpp for the annotated version). For each
// RPC it looks the target up by NodeId, queues a job that hands the message
// to that node, and -- when the job runs -- passes the node's reply back to
// the sender through the callback RaftNode supplied.
class Network : public Transport {
  public:
    void attach(const NodeId& id, RaftNode& node) {
        nodes_[id] = &node;
    }

    Status sendRequestVote(const NodeId& target, RequestVoteArgs args,
                           MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;

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
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;

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

// One cluster member. `timeout` is its election timeout in ticks;
// heartbeat is 2 ticks.
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

// Builds a three-node cluster with the given election timeouts and runs
// `rounds` rounds of 3 ticks each, delivering all RPCs at the end of every
// round. Prints every node's role and term after each one.
void runRounds(int t1, int t2, int t3, int rounds) {
    Network network;
    FixedRandom random;
    Member node1("node-1", peersOf("node-2", "node-3"), network, random, t1);
    Member node2("node-2", peersOf("node-1", "node-3"), network, random, t2);
    Member node3("node-3", peersOf("node-1", "node-2"), network, random, t3);
    Member* const members[] = {&node1, &node2, &node3};

    for (int round = 1; round <= rounds; ++round) {
        for (int tick = 0; tick < 3; ++tick) {
            for (Member* m : members)
                (void)m->raft.tick();
        }
        network.deliverAll();

        std::cout << "round " << round << " :";
        const Member* leader = nullptr;
        for (const Member* m : members) {
            std::cout << " " << m->state.selfId() << " " << roleName(m->state.role()) << " (term "
                      << m->state.currentTerm() << ")";
            if (m->state.role() == Role::Leader)
                leader = m;
        }
        std::cout << "\n         leader : " << (leader ? leader->state.selfId() : "none") << "\n";
    }
    std::cout << "\n";
}

} // namespace

static void run_examples() {
    // All three nodes time out after exactly 3 ticks, so on the same tick
    // each one bumps its term, votes for itself, and asks the others for a
    // vote. Every node has already voted (for itself) in that term, so
    // every request is denied. Nobody has a majority. The timers re-roll to
    // the same value, so the next attempt collides in exactly the same way
    // -- the term climbs every round and no leader ever appears.
    setTitle("Trap: Everyone Times Out Together");

    runRounds(3, 3, 3, 3); // (all Candidates, terms 1 to 3, no leader)

    // Give each node a different timeout and the tie can't happen: node-1
    // times out first, and by the time the others would, its heartbeats
    // have reset their timers. In production you get this from
    // SystemRandomSource and a timeout range (for example 150 to 300 ms,
    // in ticks), which is why RaftNode takes a min and a max.
    setTitle("Fix: Spread the Timeouts");

    runRounds(3, 5, 7, 2); // (node-1 Leader in round 1, stable in round 2)
}

REGISTER_EXAMPLE_SUITE();
