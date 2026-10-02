// Hello, Raft.
//
// Demonstrates:
// - the four interfaces a RaftNode is built on, implemented in memory
// - driving it with tick() instead of a clock
// - the election timeout firing and the node becoming a Candidate
// - what a Candidate has done by then: bumped its term, voted for
//   itself, and asked its peers for votes

#include <support/framework.h>

using namespace RaftCore;

namespace {

// RaftCore owns no storage, network, or randomness of its own. It talks
// to four interfaces, and these are the smallest possible in-memory
// implementations of each -- enough to run a node with no disk, sockets,
// or real randomness.

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

// Transport: sends RPCs to peers. This one just counts them and never
// delivers a reply.
class CountingTransport : public Transport {
  public:
    Status sendRequestVote(const NodeId&, RequestVoteArgs,
                           MoveOnlyFunction<void(RequestVoteReply)>) override {
        ++voteSends;
        return Status::OK;
    }
    Status sendAppendEntries(const NodeId&, AppendEntriesArgs,
                             MoveOnlyFunction<void(AppendEntriesReply)>) override {
        return Status::OK;
    }

    int voteSends = 0;
};

// RandomSource: picks each election timeout. Returning the lower bound
// makes the timeout exact.
class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

} // namespace

// Role is an enum class, so it needs a name to print.
static const char* roleName(Role role) {
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

static void run_examples() {
    // Everything is in memory, so the example is deterministic.
    setTitle("Wire Up a Node");

    MemoryPersistentState persistent; // starts at term 0, no vote cast
    MemoryStorage log;
    CountingTransport transport;
    FixedRandom random;

    // NodeState holds the role plus the Raft state; RaftNode adds the
    // election and replication logic on top of it. This node is "node-1"
    // in a three-node cluster.
    NodeState state(persistent, log, "node-1");

    Vector<NodeId> peers;
    peers.push_back(NodeId("node-2"));
    peers.push_back(NodeId("node-3"));

    // Election timeout: 5 ticks (min == max, so it's exact -- real
    // deployments use a range so nodes don't all time out together, see
    // misuse/identical_timeouts.cpp). Heartbeat: every 2 ticks, once Leader.
    RaftNode raft(state, transport, random, peers, 5, 5, 2);

    std::cout << "role : " << roleName(state.role()) << "\n";
    std::cout << "term : " << state.currentTerm() << "\n\n";

    // There's no clock: time only advances when you call tick(). Each
    // call is one unit of time, so the caller decides how long a tick is
    // (and tests never have to sleep).
    setTitle("Tick Toward the Timeout");

    for (int i = 1; i <= 5; ++i) {
        Status s = raft.tick();
        std::cout << "tick " << i << " status : " << static_cast<int>(s) // (OK)
                  << ", role : " << roleName(state.role()) << "\n";
    }
    std::cout << "\n";

    // Nothing arrived from a leader within 5 ticks, so on the fifth the
    // node started an election: it bumped its term, voted for itself, and
    // sent a RequestVote to each peer.
    setTitle("See the Election");

    std::cout << "role              : " << roleName(state.role()) << "\n";
    std::cout << "term              : " << state.currentTerm() << "\n";
    std::cout << "voted for         : " << state.votedFor().value_or("(nobody)") << "\n";
    std::cout << "RequestVotes sent : " << transport.voteSends << "\n\n";

    // The fake transport doesn't deliver replies, so no votes come back
    // and the node stays a Candidate. Feeding replies in is what makes it
    // a Leader -- see basic_election.cpp.
    std::cout << "No replies arrived, so the node is still a " << roleName(state.role()) << ".\n";
}

REGISTER_EXAMPLE_SUITE();
