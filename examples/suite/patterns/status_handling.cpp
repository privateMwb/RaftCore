// Status Handling.
//
// Demonstrates:
// - checking the [[nodiscard]] Status that tick() and the handlers return
// - what a failed tick leaves behind (nothing changes) and how it retries
// - propagating the first failure with an early return
// - discarding a Status on purpose, and when that's safe
// - reply handlers that return void: takeLastAsyncError()

#include <support/framework.h>

using namespace RaftCore;

namespace {

// Storage and Transport are the same in-memory versions as the quickstart
// examples. The interesting one here is the PersistentState below: it can
// be told to start failing, like a disk that fills up.

// PersistentState with a fault switch: while failWrites is true, every write
// returns IO_ERROR and stores nothing.
class FlakyPersistentState : public PersistentState {
  public:
    Status setCurrentTerm(Term term) override {
        if (failWrites)
            return Status::IO_ERROR;
        term_ = term;
        return Status::OK;
    }
    Term currentTerm() const override {
        return term_;
    }

    Status setVotedFor(std::optional<NodeId> candidate) override {
        if (failWrites)
            return Status::IO_ERROR;
        votedFor_ = std::move(candidate);
        return Status::OK;
    }
    std::optional<NodeId> votedFor() const override {
        return votedFor_;
    }

    bool failWrites = false;

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
    explicit NodeRig(int timeoutTicks)
        : state(persistent, storage, "node-1"),
          raft(state, transport, random, makePeers(), timeoutTicks, timeoutTicks, 2) {}

    FlakyPersistentState persistent;
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

const char* statusName(Status status) {
    switch (status) {
    case Status::OK:
        return "OK";
    case Status::IO_ERROR:
        return "IO_ERROR";
    case Status::NOT_FOUND:
        return "NOT_FOUND";
    case Status::OUT_OF_MEMORY:
        return "OUT_OF_MEMORY";
    case Status::PARSE_ERROR:
        return "PARSE_ERROR";
    }
    return "?";
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

// Runs up to `count` ticks and stops at the first failure, returning that
// Status. `completed` says how many ticks ran, including the failing one.
// This is the usual shape for any helper that calls something [[nodiscard]]
// more than once: check, and pass the first failure up.
Status runTicks(RaftNode& raft, int count, int& completed) {
    completed = 0;
    for (int i = 0; i < count; ++i) {
        Status status = raft.tick();
        ++completed;
        if (status != Status::OK)
            return status;
    }
    return Status::OK;
}

void printTick(int number, Status status, const NodeRig& node) {
    std::cout << "tick " << number << " : " << statusName(status) << ", role "
              << roleName(node.state.role()) << ", term " << node.state.currentTerm() << "\n";
}

} // namespace

static void run_examples() {
    // Anything that touches storage can fail, so tick() and the RPC
    // handlers return a [[nodiscard]] Status -- ignoring one is a compiler
    // warning. Here the disk starts rejecting writes; the node has a 3-tick
    // election timeout, so the third tick tries to start an election and
    // can't persist the new term.
    setTitle("Check Every Status");

    NodeRig first(3);
    first.persistent.failWrites = true;

    for (int i = 1; i <= 3; ++i) {
        Status status = first.raft.tick();
        printTick(i, status, first);
    }

    // A failed election leaves term, vote, and role exactly as they were,
    // and the timeout is re-rolled so the node doesn't retry on every
    // tick. Once the disk recovers, the next timeout succeeds by itself:
    // there is nothing to undo.
    std::cout << "\ndisk recovers\n";
    first.persistent.failWrites = false;

    for (int i = 4; i <= 6; ++i) {
        Status status = first.raft.tick();
        printTick(i, status, first);
    }
    std::cout << "\n";

    // When your own function runs several ticks, hand the first failure
    // back to the caller instead of pressing on.
    setTitle("Pass the First Failure Up");

    NodeRig second(3);
    second.persistent.failWrites = true;

    int completed = 0;
    Status status = runTicks(second.raft, 10, completed);
    std::cout << "asked for 10 ticks, stopped after " << completed << " : " << statusName(status)
              << "\n\n"; // (tick 3, IO_ERROR)

    // Sometimes a failed tick really doesn't matter: tick() retries itself
    // on a later tick, so a loop that only advances time can discard the
    // Status -- but do it with an explicit (void), so the choice is visible.
    // Don't do this with handleRequestVote() or handleAppendEntries(): a
    // failure there means the reply must not be sent.
    setTitle("Discard on Purpose");

    NodeRig third(2);
    third.persistent.failWrites = true;

    for (int i = 0; i < 4; ++i)
        (void)third.raft.tick(); // Two elections attempted, both failed.

    std::cout << "after 4 failed ticks : role " << roleName(third.state.role()) << ", term "
              << third.state.currentTerm() << "\n"; // (still Follower, term 0)

    third.persistent.failWrites = false;
    for (int i = 0; i < 2; ++i)
        (void)third.raft.tick();

    std::cout << "after recovery       : role " << roleName(third.state.role()) << ", term "
              << third.state.currentTerm() << "\n\n"; // (Candidate, term 1)

    // handleRequestVoteReply() runs inside a Transport callback, whose
    // signature is void(Reply) -- there's nowhere to return a Status. A
    // persistence failure while handling it is stashed instead, and
    // takeLastAsyncError() hands it back once and clears it. Poll it from
    // your event loop if you care.
    setTitle("Read the Async Error");

    NodeRig fourth(1);
    (void)fourth.raft.tick(); // Becomes Candidate at term 1.

    // A reply from a newer term (5) means this node must adopt it -- and
    // adopting it means writing the term, which fails.
    fourth.persistent.failWrites = true;
    RequestVoteReply reply;
    reply.term = 5;
    reply.voteGranted = false;
    fourth.raft.handleRequestVoteReply("node-2", reply);

    std::optional<Status> error = fourth.raft.takeLastAsyncError();
    std::cout << "async error : " << (error ? statusName(*error) : "none") << "\n"; // (IO_ERROR)

    error = fourth.raft.takeLastAsyncError();
    std::cout << "again       : " << (error ? statusName(*error) : "none") << "\n"; // (none)
}

REGISTER_EXAMPLE_SUITE();
