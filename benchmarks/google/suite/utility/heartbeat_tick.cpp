// Heartbeat Tick Benchmark Suite
// Measures tick() on the Leader path, on a 5-node leader with a
// 10-entry log: the idle counter check, a tick that sends a heartbeat
// to every peer, and the amortized mix of the two at a realistic
// heartbeat-to-tick ratio.
//
// Covers:
// - idle leader tick (heartbeat interval far away)
// - heartbeat every tick (4 AppendEntries sends, each with a prev-entry
//   log read and nextIndex lookup)
// - heartbeat every 10th tick (each timed call is one tick, so the
//   result is the average per tick: nine idle, one heartbeat)

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures a leader tick that only advances the heartbeat counter.
static void heartbeat_tick_lead_idle(benchmark::State& state) {
    Rig rig(10, 150, 4, 2000000000);
    rig.node.becomeLeader(makePeers());

    for (auto _ : state) {
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(heartbeat_tick_lead_idle);

// Measures a leader tick that sends a full round of heartbeats.
static void heartbeat_tick_lead_beat(benchmark::State& state) {
    Rig rig(10, 150, 4, 1);
    rig.node.becomeLeader(makePeers());

    for (auto _ : state) {
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(heartbeat_tick_lead_beat);

// Measures the average leader tick when a heartbeat goes out every 10
// ticks.
static void heartbeat_tick_lead_one_in_ten(benchmark::State& state) {
    Rig rig(10, 150, 4, 10);
    rig.node.becomeLeader(makePeers());

    for (auto _ : state) {
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(heartbeat_tick_lead_one_in_ten);
