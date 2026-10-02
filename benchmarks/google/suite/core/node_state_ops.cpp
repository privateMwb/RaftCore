// Node State Ops Benchmark Suite
// Measures NodeState's role/term transitions: the persistence writes
// (currentTerm, votedFor) each one triggers, and the leader-state
// rebuild on becomeLeader().
//
// Covers:
// - observeTerm stale (no-op) vs newer (two persistence writes)
// - grantVoteTo (one persistence write, NodeId copy)
// - startElection (term bump + self vote)
// - becomeLeader (rebuilds nextIndex/matchIndex for 4 peers)
// - stepDownToFollower (follower path: nothing to drop)

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures observeTerm() with an older term -- the early-out.
static void node_state_ops_observe_stale(benchmark::State& state) {
    Rig rig;

    for (auto _ : state) {
        Status s = rig.node.observeTerm(1);
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(node_state_ops_observe_stale);

// Measures observeTerm() with a newer term every call -- adopts the
// term and clears the vote.
static void node_state_ops_observe_newer(benchmark::State& state) {
    Rig rig;
    Term t = 100;

    for (auto _ : state) {
        Status s = rig.node.observeTerm(++t);
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(node_state_ops_observe_newer);

// Measures grantVoteTo() -- one setVotedFor() plus the NodeId copy.
static void node_state_ops_grant_vote(benchmark::State& state) {
    Rig rig;
    const NodeId candidate("node-2");

    for (auto _ : state) {
        Status s = rig.node.grantVoteTo(candidate);
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(node_state_ops_grant_vote);

// Measures startElection() -- bumps the term and votes for self each call.
static void node_state_ops_start_election(benchmark::State& state) {
    Rig rig;

    for (auto _ : state) {
        Status s = rig.node.startElection();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(node_state_ops_start_election);

// Measures becomeLeader() over a 4-peer list -- builds both per-peer
// HashMaps from scratch, replacing the previous leader state.
static void node_state_ops_become_leader(benchmark::State& state) {
    Rig rig(10);
    const Vector<NodeId> peers = makePeers();

    for (auto _ : state) {
        rig.node.becomeLeader(peers);
    }
}
BENCHMARK(node_state_ops_become_leader);

// Measures stepDownToFollower() on a Follower -- role write plus an
// already-empty leader-state reset.
static void node_state_ops_step_down(benchmark::State& state) {
    Rig rig;

    for (auto _ : state) {
        rig.node.stepDownToFollower();
    }
}
BENCHMARK(node_state_ops_step_down);
