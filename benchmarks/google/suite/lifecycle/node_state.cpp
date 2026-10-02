// Node State Lifecycle Benchmark Suite
// Measures construction and move of NodeState, both as a fresh Follower
// and as a Leader carrying per-peer replication state (two HashMaps
// over 4 peers).
//
// NodeState holds references to its PersistentState/Storage, so it is
// move-constructible but not assignable, and a moved-from object can't
// be reused as a source. Every move case therefore builds its source
// inside the timed call. Subtract the matching build-only case to get
// the move cost: node_state_construct_move - node_state_construct, and
// node_state_leader_move - node_state_leader_build.
//
// Covers:
// - construction (Follower)
// - construction + move (Follower)
// - construction + becomeLeader (baseline for the leader move)
// - construction + becomeLeader + move

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures constructing a Follower NodeState (including NodeId copy-in
// and destruction).
static void node_state_construct(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st(10);

    for (auto _ : state) {
        NodeState n(ps, st, "node-1");
        benchmark::DoNotOptimize(n);
    }
}
BENCHMARK(node_state_construct);

// Measures constructing a Follower and move-constructing from it.
static void node_state_construct_move(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st(10);

    for (auto _ : state) {
        NodeState a(ps, st, "node-1");
        NodeState b(std::move(a));
        benchmark::DoNotOptimize(b);
    }
}
BENCHMARK(node_state_construct_move);

// Baseline for the leader move: construction plus becomeLeader() over 4
// peers.
static void node_state_leader_build(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st(10);
    const Vector<NodeId> peers = makePeers();

    for (auto _ : state) {
        NodeState a(ps, st, "node-1");
        a.becomeLeader(peers);
        benchmark::DoNotOptimize(a);
    }
}
BENCHMARK(node_state_leader_build);

// Measures the same plus a move-construction, which carries the two
// populated HashMaps across.
static void node_state_leader_move(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st(10);
    const Vector<NodeId> peers = makePeers();

    for (auto _ : state) {
        NodeState a(ps, st, "node-1");
        a.becomeLeader(peers);
        NodeState b(std::move(a));
        benchmark::DoNotOptimize(b);
    }
}
BENCHMARK(node_state_leader_move);
