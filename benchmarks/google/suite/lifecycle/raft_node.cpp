// Raft Node Lifecycle Benchmark Suite
// Measures constructing (and destroying) a RaftNode: taking the peer
// list by value, and the initial election-timer roll through
// RandomSource.
//
// RaftNode is deliberately not benchmarked for move: in-flight Transport
// callbacks capture `this`, so a live RaftNode must never be relocated.
//
// Covers:
// - construction with an empty peer list
// - construction with a 4-peer list (list built inside the timed call)
// - building the 4-peer list alone (baseline: subtract from the above)

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures constructing a RaftNode with no peers.
static void raft_node_ctor_no_peers(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    FakeTransport transport;
    FakeRandom random;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        RaftNode raft(node, transport, random, Vector<NodeId>{}, 150, 300, 50);
        benchmark::DoNotOptimize(raft);
    }
}
BENCHMARK(raft_node_ctor_no_peers);

// Measures constructing a RaftNode with a freshly built 4-peer list
// moved in.
static void raft_node_ctor_four_peers(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    FakeTransport transport;
    FakeRandom random;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        RaftNode raft(node, transport, random, makePeers(), 150, 300, 50);
        benchmark::DoNotOptimize(raft);
    }
}
BENCHMARK(raft_node_ctor_four_peers);

// Baseline: building and destroying the 4-peer list on its own.
static void raft_node_peers_build(benchmark::State& state) {

    for (auto _ : state) {
        Vector<NodeId> peers = makePeers();
        benchmark::DoNotOptimize(peers);
    }
}
BENCHMARK(raft_node_peers_build);
