// Tick Driver.
//
// Demonstrates:
// - RaftCore having no clock: time only advances when you call tick()
// - choosing a tick length and turning millisecond timeouts into ticks
// - driving a node in a plain loop, with no sleeping
// - a small driver that converts real elapsed time into ticks, carrying the
//   remainder and capping catch-up after a long stall

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
    NodeRig(int electionMinTicks, int electionMaxTicks, int heartbeatTicks)
        : state(persistent, storage, "node-1"),
          raft(state, transport, random, makePeers(), electionMinTicks, electionMaxTicks,
               heartbeatTicks) {}

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

// Turns real elapsed time into tick() calls. Call advance() from whatever
// timer or event loop the application already has, with the time that has
// passed since the last call.
//
// - The remainder that doesn't make a whole tick is carried to the next call.
// - After a long stall (a suspended laptop, a paused debugger), replaying
//   every missed tick would make the node hallucinate a burst of timeouts,
//   so at most `maxCatchUp` ticks run per call and the backlog is dropped.
class TickDriver {
  public:
    TickDriver(RaftNode& raft, int tickMs, int maxCatchUp)
        : raft_(raft), tickMs_(tickMs), maxCatchUp_(maxCatchUp) {}

    // Returns how many ticks ran.
    int advance(int elapsedMs) {
        pendingMs_ += elapsedMs;
        int due = pendingMs_ / tickMs_;
        pendingMs_ %= tickMs_;
        if (due > maxCatchUp_)
            due = maxCatchUp_;

        // A failed tick retries itself later -- see status_handling.cpp.
        for (int i = 0; i < due; ++i)
            (void)raft_.tick();
        return due;
    }

  private:
    RaftNode& raft_;
    int tickMs_;
    int maxCatchUp_;
    int pendingMs_ = 0;
};

} // namespace

static void run_examples() {
    // RaftCore spawns no timer threads and never reads a clock. Timeouts
    // are counted in ticks, and one tick is however long you decide. Pick
    // a tick length, then express every timeout in whole ticks. Keep the
    // heartbeat well under the minimum election timeout, or followers will
    // time out between heartbeats.
    setTitle("Choose a Tick Length");

    const int tickMs = 10;
    const int electionMinMs = 150;
    const int electionMaxMs = 300;
    const int heartbeatMs = 50;

    const int electionMinTicks = electionMinMs / tickMs;
    const int electionMaxTicks = electionMaxMs / tickMs;
    const int heartbeatTicks = heartbeatMs / tickMs;

    std::cout << "tick length : " << tickMs << " ms\n";
    std::cout << "election    : " << electionMinTicks << " to " << electionMaxTicks
              << " ticks\n";                                               // (15 to 30)
    std::cout << "heartbeat   : every " << heartbeatTicks << " ticks\n\n"; // (5)

    // In a test or a simulation, the loop itself is the clock: call tick()
    // as fast as you like, and 150 ms of "Raft time" passes in an instant.
    // FixedRandom always picks the minimum, so the timeout here is exactly
    // 15 ticks.
    setTitle("Drive a Node in a Loop");

    NodeRig simulated(electionMinTicks, electionMaxTicks, heartbeatTicks);

    int ticks = 0;
    while (simulated.state.role() == Role::Follower && ticks < 100) {
        (void)simulated.raft.tick();
        ++ticks;
    }
    std::cout << "became " << roleName(simulated.state.role()) << " after " << ticks
              << " ticks = " << ticks * tickMs << " ms of Raft time\n\n"; // (15 ticks, 150 ms)

    // In a real application, wall-clock time arrives in irregular chunks.
    // The driver above converts it: it carries leftover milliseconds
    // between calls, and caps how many ticks one call may replay.
    setTitle("Convert Elapsed Time to Ticks");

    NodeRig real(electionMinTicks, electionMaxTicks, heartbeatTicks);
    TickDriver driver(real.raft, tickMs, 5);

    int total = 0;
    const int elapsed[] = {7, 7, 100, 1000, 50};
    for (int ms : elapsed) {
        int ran = driver.advance(ms);
        total += ran;
        std::cout << "advance(" << ms << " ms) ran " << ran << " ticks (total " << total << ")\n";
    }
    // (0, 1, 5, 5, 5: the first two calls leave 7 + 7 = 14 ms, one tick
    // plus 4 ms carried over; the two big calls hit the cap of 5.)

    std::cout << "\nrole after " << total << " ticks : " << roleName(real.state.role())
              << "\n"; // (Candidate: the 16th tick passed the 15-tick timeout)
}

REGISTER_EXAMPLE_SUITE();
