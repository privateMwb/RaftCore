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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures constructing a RaftNode with no peers.
static void bench_ctor_no_peers() {
    FakePersistentState ps;
    FakeStorage st;
    FakeTransport transport;
    FakeRandom random;
    NodeState node(ps, st, "node-1");

    auto build = [&] {
        RaftNode raft(node, transport, random, Vector<NodeId>{}, 150, 300, 50);
        doNotOptimize(raft);
    };
    BENCH_SOLO("ctor 0 peer", build);
}

// Measures constructing a RaftNode with a freshly built 4-peer list
// moved in.
static void bench_ctor_four_peers() {
    FakePersistentState ps;
    FakeStorage st;
    FakeTransport transport;
    FakeRandom random;
    NodeState node(ps, st, "node-1");

    auto build = [&] {
        RaftNode raft(node, transport, random, makePeers(), 150, 300, 50);
        doNotOptimize(raft);
    };
    BENCH_SOLO("ctor 4 peer", build);
}

// Baseline: building and destroying the 4-peer list on its own.
static void bench_peers_build() {
    auto build = [&] {
        Vector<NodeId> peers = makePeers();
        doNotOptimize(peers);
    };
    BENCH_SOLO("peers 4", build);
}

// Executes all RaftNode lifecycle benchmark cases.
static void run_benchmarks() {
    bench_ctor_no_peers();
    bench_ctor_four_peers();
    bench_peers_build();
}

REGISTER_BENCH_SUITE();
