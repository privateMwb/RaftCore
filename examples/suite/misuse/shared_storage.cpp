// Shared Storage.
//
// Demonstrates:
// - two nodes built on one PersistentState reading and overwriting each
//   other's term and vote
// - two nodes built on one Storage seeing each other's log entries, and
//   one node's truncation deleting the other's
// - the fix: every node gets its own PersistentState and Storage
//
// RaftCore takes PersistentState and Storage by reference and never checks
// who else is using them. Sharing compiles and runs -- it just breaks the
// assumption every Raft rule rests on: each node's term, vote, and log are
// its own.

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

// Transport: sends RPCs to peers. This one accepts every RPC and drops it,
// so no reply ever comes back.
class NullTransport : public Transport {
  public:
    Status sendRequestVote(const NodeId&, RequestVoteArgs,
                           MoveOnlyFunction<void(RequestVoteReply)>) override {
        return Status::OK;
    }
    Status sendAppendEntries(const NodeId&, AppendEntriesArgs,
                             MoveOnlyFunction<void(AppendEntriesReply)>) override {
        return Status::OK;
    }
};

// RandomSource: picks each election timeout. Returning the lower bound
// makes it exact.
class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// One node in a two-node cluster, built on whatever PersistentState and
// Storage it's handed -- which is exactly what makes sharing possible.
struct NodeRig {
    NodeRig(const NodeId& id, const NodeId& other, PersistentState& persistent, Storage& storage)
        : state(persistent, storage, id), raft(state, transport, random, peersOf(other), 1, 1, 2) {}

    NullTransport transport;
    FixedRandom random;
    NodeState state;
    RaftNode raft;

  private:
    static Vector<NodeId> peersOf(const NodeId& other) {
        Vector<NodeId> peers;
        peers.push_back(other);
        return peers;
    }
};

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

void printNode(const NodeRig& node) {
    std::cout << node.state.selfId() << " : " << roleName(node.state.role()) << ", term "
              << node.state.currentTerm() << ", voted for " << node.state.votedFor().value_or("-")
              << ", log " << node.state.log().lastIndex() << "\n";
}

} // namespace

static void run_examples() {
    // Both nodes are built on the same PersistentState. node-1 times out
    // and starts an election -- and node-2, which has heard nothing from
    // anyone, is suddenly at term 1 having "voted" for node-1. Then node-2
    // times out: it starts its own election, and node-1, still a
    // Candidate, finds its term has jumped to 2 and its vote has been given
    // to node-2. Two candidates can now hold the same term with votes
    // that were never requested or granted.
    setTitle("Trap: One PersistentState, Two Nodes");

    {
        MemoryPersistentState shared;
        MemoryStorage storageA;
        MemoryStorage storageB;
        NodeRig nodeA("node-1", "node-2", shared, storageA);
        NodeRig nodeB("node-2", "node-1", shared, storageB);

        (void)nodeA.raft.tick();
        std::cout << "after node-1 times out\n";
        printNode(nodeA); // (Candidate, term 1, voted for node-1)
        printNode(nodeB); // (Follower, term 1, voted for node-1 -- it heard nothing)

        (void)nodeB.raft.tick();
        std::cout << "\nafter node-2 times out\n";
        printNode(nodeA); // (still Candidate, but term 2, voted for node-2)
        printNode(nodeB); // (Candidate, term 2, voted for node-2)
        std::cout << "\n";
    }

    // Now the two nodes share a Storage. An entry appended to node-1's log
    // appears in node-2's with no AppendEntries ever sent, so replication
    // has nothing left to do -- and the safety checks are answering from
    // a log that was never replicated. Worse, when node-2 truncates a
    // conflicting suffix (which is a normal part of catching up), it
    // deletes node-1's entries too.
    setTitle("Trap: One Storage, Two Nodes");

    {
        MemoryStorage shared;
        MemoryPersistentState persistentA;
        MemoryPersistentState persistentB;
        NodeRig nodeA("node-1", "node-2", persistentA, shared);
        NodeRig nodeB("node-2", "node-1", persistentB, shared);

        LogIndex index = kNoIndex;
        (void)nodeA.state.log().append(1, Vector<std::uint8_t>{}, index);
        std::cout << "node-1 appended entry " << index << "\n";
        printNode(nodeA); // (log 1)
        printNode(nodeB); // (log 1 -- never replicated)

        (void)nodeB.state.log().truncateFrom(1);
        std::cout << "\nnode-2 truncates from index 1\n";
        printNode(nodeA); // (log 0 -- node-1's entry is gone)
        printNode(nodeB);
        std::cout << "\n";
    }

    // One PersistentState and one Storage per node. The same steps as the
    // first trap now leave the other node alone.
    setTitle("Fix: One Set Per Node");

    {
        MemoryPersistentState persistentA;
        MemoryPersistentState persistentB;
        MemoryStorage storageA;
        MemoryStorage storageB;
        NodeRig nodeA("node-1", "node-2", persistentA, storageA);
        NodeRig nodeB("node-2", "node-1", persistentB, storageB);

        (void)nodeA.raft.tick();
        printNode(nodeA); // (Candidate, term 1, voted for node-1)
        printNode(nodeB); // (Follower, term 0, voted for nobody)
    }
}

REGISTER_EXAMPLE_SUITE();
