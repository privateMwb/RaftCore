// Election Timer Benchmark Suite
// Measures tick() on the Follower/Candidate path: the per-tick counter
// bump that runs on every non-leader node every tick, the timeout that
// turns into an election, and the steady state of a healthy follower
// (ticking while heartbeats keep resetting its timer).
//
// Covers:
// - idle tick (timeout far away: counter increment and compare)
// - firing tick (1-tick timeout: every call starts an election, i.e. a
//   Candidate re-electing after a split vote, with 4 RequestVote sends)
// - healthy follower: tick + a valid heartbeat, timer reset each round

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures a tick that does nothing but advance the counter. The timeout
// is set near INT_MAX so it never fires during a run.
static void bench_tick_idle() {
    Rig rig(0, 2000000000);

    auto idle = [&] {
        Status s = rig.raft.tick();
        doNotOptimize(s);
    };
    BENCH_SOLO("tick idle", idle);
}

// Measures a tick that always times out: term bump, self vote, timer
// re-roll, and one RequestVote (args copy) per peer.
static void bench_tick_fire() {
    Rig rig(0, 1);

    auto fire = [&] {
        Status s = rig.raft.tick();
        doNotOptimize(s);
    };
    BENCH_SOLO("tick fire", fire);
}

// Measures one round of a healthy follower: a tick, then the leader's
// heartbeat arrives and resets the timer, so the timeout never fires.
// Subtract "tick idle" (and the core suite's "ae beat") to see what the
// combination costs beyond its parts.
static void bench_tick_beat() {
    Rig rig(0, 150);
    AppendEntriesReply reply;

    auto round = [&] {
        Status t = rig.raft.tick();
        doNotOptimize(t);

        AppendEntriesArgs args;
        args.term = 7;
        args.leaderId = "node-2";
        Status h = rig.raft.handleAppendEntries(std::move(args), reply);
        doNotOptimize(h);
        doNotOptimize(reply);
    };
    BENCH_SOLO("tick+beat", round);
}

// Executes all election-timer benchmark cases.
static void run_benchmarks() {
    bench_tick_idle();
    bench_tick_fire();
    bench_tick_beat();
}

REGISTER_BENCH_SUITE();
