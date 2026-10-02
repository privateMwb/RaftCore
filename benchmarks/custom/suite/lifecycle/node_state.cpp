// Node State Lifecycle Benchmark Suite
// Measures construction and move of NodeState, both as a fresh Follower
// and as a Leader carrying per-peer replication state (two HashMaps
// over 4 peers).
//
// NodeState holds references to its PersistentState/Storage, so it is
// move-constructible but not assignable, and a moved-from object can't
// be reused as a source. Every move case therefore builds its source
// inside the timed call. Subtract the matching build-only case to get
// the move cost: "ctor+move" - "construct", "lead+move" - "lead ctor".
//
// Covers:
// - construction (Follower)
// - construction + move (Follower)
// - construction + becomeLeader (baseline for the leader move)
// - construction + becomeLeader + move

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures constructing a Follower NodeState (including NodeId copy-in
// and destruction).
static void bench_construct() {
    FakePersistentState ps;
    FakeStorage st(10);

    auto build = [&] {
        NodeState n(ps, st, "node-1");
        doNotOptimize(n);
    };
    BENCH_SOLO("construct", build);
}

// Measures constructing a Follower and move-constructing from it.
static void bench_construct_move() {
    FakePersistentState ps;
    FakeStorage st(10);

    auto buildMove = [&] {
        NodeState a(ps, st, "node-1");
        NodeState b(std::move(a));
        doNotOptimize(b);
    };
    BENCH_SOLO("ctor+move", buildMove);
}

// Baseline for the leader move: construction plus becomeLeader() over 4
// peers.
static void bench_leader_build() {
    FakePersistentState ps;
    FakeStorage st(10);
    const Vector<NodeId> peers = makePeers();

    auto build = [&] {
        NodeState a(ps, st, "node-1");
        a.becomeLeader(peers);
        doNotOptimize(a);
    };
    BENCH_SOLO("lead ctor", build);
}

// Measures the same plus a move-construction, which carries the two
// populated HashMaps across.
static void bench_leader_move() {
    FakePersistentState ps;
    FakeStorage st(10);
    const Vector<NodeId> peers = makePeers();

    auto buildMove = [&] {
        NodeState a(ps, st, "node-1");
        a.becomeLeader(peers);
        NodeState b(std::move(a));
        doNotOptimize(b);
    };
    BENCH_SOLO("lead+move", buildMove);
}

// Executes all NodeState lifecycle benchmark cases.
static void run_benchmarks() {
    bench_construct();
    bench_construct_move();
    bench_leader_build();
    bench_leader_move();
}

REGISTER_BENCH_SUITE();
