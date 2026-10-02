// Stale Term Ignored.
//
// Demonstrates:
// - a granted vote from an older term does not count toward a majority
// - an AppendEntries from an older-term leader is rejected, not obeyed
// - a rejection carrying a higher term is how a stale leader steps down
//
// The trap: RaftNode drops or refuses anything stamped with an old term,
// so a late message never has the effect its content suggests. The fix is
// mostly to let RaftCore see every reply, rejections included.

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

// One node ("node-1" with two peers) and everything it's wired to.
struct NodeRig {
    NodeRig(int timeoutTicks, Term startTerm)
        : state(persistent, storage, "node-1"),
          raft(state, transport, random, makePeers(), timeoutTicks, timeoutTicks, 2) {
        (void)persistent.setCurrentTerm(startTerm);
    }

    MemoryPersistentState persistent;
    MemoryStorage storage;
    NullTransport transport;
    FixedRandom random;
    NodeState state;
    RaftNode raft;

  private:
    static Vector<NodeId> makePeers() {
        Vector<NodeId> peers;
        peers.push_back(NodeId("node-2"));
        peers.push_back(NodeId("node-3"));
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

} // namespace

static void run_examples() {
    // node-1 times out and becomes a Candidate for term 1. Two votes would
    // be a majority in a three-node cluster, and two "granted" replies now
    // arrive -- but stamped term 0, left over from an election that
    // finished long ago (a delayed or duplicated packet). Counting them
    // would let a node win with votes nobody cast for this election.
    setTitle("Trap: Counting a Late Vote");

    NodeRig candidate(1, 0);
    (void)candidate.raft.tick(); // Candidate, term 1.

    RequestVoteReply late;
    late.term = 0;
    late.voteGranted = true;
    candidate.raft.handleRequestVoteReply("node-2", late);
    candidate.raft.handleRequestVoteReply("node-3", late);

    std::cout << "after 2 late votes : " << roleName(candidate.state.role()) << ", term "
              << candidate.state.currentTerm() << "\n\n"; // (still a Candidate, term 1)

    // A follower at term 5 is told to append an entry by a "leader" of
    // term 3 -- a leader that lost its position and hasn't noticed. The
    // follower refuses: success is false, and the reply carries term 5 so
    // the sender can learn it's out of date. The entry is not appended.
    setTitle("Trap: Obeying an Old Leader");

    NodeRig follower(3, 5);

    AppendEntriesArgs args;
    args.term = 3;
    args.leaderId = "node-2";
    LogEntry entry;
    entry.term = 3;
    args.entries.push_back(std::move(entry));

    AppendEntriesReply reply;
    Status status = follower.raft.handleAppendEntries(std::move(args), reply);
    std::cout << "handler returned : " << (status == Status::OK ? "OK" : "error") << "\n"; // (OK)
    std::cout << "reply            : success " << (reply.success ? "true" : "false") << ", term "
              << reply.term << "\n"; // (false, term 5)
    std::cout << "follower         : term " << follower.state.currentTerm() << ", log "
              << follower.state.log().lastIndex() << " entries\n\n"; // (term 5, 0 entries)

    // Only a vote for the current term counts -- here it takes one grant
    // from node-2 plus node-1's own vote to make a majority of three.
    // And a leader that hears about a higher term steps down immediately:
    // the rejection from a follower at term 7 is not an error to retry
    // but the news that this leader is stale. So pass every reply, including
    // failures, to the handler -- dropping them means a stale leader never
    // finds out.
    setTitle("Fix: Let RaftCore See Every Reply");

    RequestVoteReply current;
    current.term = candidate.state.currentTerm();
    current.voteGranted = true;
    candidate.raft.handleRequestVoteReply("node-2", current);
    std::cout << "after 1 current vote : " << roleName(candidate.state.role()) << "\n"; // (Leader)

    AppendEntriesReply rejection;
    rejection.term = 7;
    rejection.success = false;
    candidate.raft.handleAppendEntriesReply("node-2", rejection);
    std::cout << "after a term-7 reply : " << roleName(candidate.state.role()) << ", term "
              << candidate.state.currentTerm() << "\n"; // (Follower, term 7)
}

REGISTER_EXAMPLE_SUITE();
