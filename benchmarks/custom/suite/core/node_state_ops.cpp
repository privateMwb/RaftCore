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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures observeTerm() with an older term -- the early-out.
static void bench_observe_stale() {
    Rig rig;

    auto stale = [&] {
        Status s = rig.node.observeTerm(1);
        doNotOptimize(s);
    };
    BENCH_SOLO("obs stale", stale);
}

// Measures observeTerm() with a newer term every call -- adopts the
// term and clears the vote.
static void bench_observe_newer() {
    Rig rig;
    Term t = 100;

    auto newer = [&] {
        Status s = rig.node.observeTerm(++t);
        doNotOptimize(s);
    };
    BENCH_SOLO("obs newer", newer);
}

// Measures grantVoteTo() -- one setVotedFor() plus the NodeId copy.
static void bench_grant_vote() {
    Rig rig;
    const NodeId candidate("node-2");

    auto grant = [&] {
        Status s = rig.node.grantVoteTo(candidate);
        doNotOptimize(s);
    };
    BENCH_SOLO("grant vote", grant);
}

// Measures startElection() -- bumps the term and votes for self each call.
static void bench_start_election() {
    Rig rig;

    auto startE = [&] {
        Status s = rig.node.startElection();
        doNotOptimize(s);
    };
    BENCH_SOLO("start elect", startE);
}

// Measures becomeLeader() over a 4-peer list -- builds both per-peer
// HashMaps from scratch, replacing the previous leader state.
static void bench_become_leader() {
    Rig rig(10);
    const Vector<NodeId> peers = makePeers();

    auto lead = [&] { rig.node.becomeLeader(peers); };
    BENCH_SOLO("become lead", lead);
}

// Measures stepDownToFollower() on a Follower -- role write plus an
// already-empty leader-state reset.
static void bench_step_down() {
    Rig rig;

    auto down = [&] { rig.node.stepDownToFollower(); };
    BENCH_SOLO("step down", down);
}

// Executes all NodeState transition benchmark cases.
static void run_benchmarks() {
    bench_observe_stale();
    bench_observe_newer();
    bench_grant_vote();
    bench_start_election();
    bench_become_leader();
    bench_step_down();
}

REGISTER_BENCH_SUITE();
