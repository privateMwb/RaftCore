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

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures a tick that does nothing but advance the counter. The timeout
// is set near INT_MAX so it never fires during a run.
static void election_timer_tick_idle(benchmark::State& state) {
    Rig rig(0, 2000000000);

    for (auto _ : state) {
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(election_timer_tick_idle);

// Measures a tick that always times out: term bump, self vote, timer
// re-roll, and one RequestVote (args copy) per peer.
static void election_timer_tick_fire(benchmark::State& state) {
    Rig rig(0, 1);

    for (auto _ : state) {
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(election_timer_tick_fire);

// Measures one round of a healthy follower: a tick, then the leader's
// heartbeat arrives and resets the timer, so the timeout never fires.
// Subtract election_timer_tick_idle (and append_entries_ae_beat) to see what the
// combination costs beyond its parts.
static void election_timer_tick_beat(benchmark::State& state) {
    Rig rig(0, 150);
    AppendEntriesReply reply;

    for (auto _ : state) {
        Status t = rig.raft.tick();
        benchmark::DoNotOptimize(t);

        AppendEntriesArgs args;
        args.term = 7;
        args.leaderId = "node-2";
        Status h = rig.raft.handleAppendEntries(std::move(args), reply);
        benchmark::DoNotOptimize(h);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(election_timer_tick_beat);
