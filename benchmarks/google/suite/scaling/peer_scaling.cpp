// Peer Scaling Benchmark Suite
// Measures how leader and candidate work grows with the number of peers
// (2, 10 and 100 peers = 3-, 11- and 101-node clusters). Each of these
// paths loops over every peer, so cost should grow roughly linearly;
// the per-peer HashMap work (string hashing) and the per-peer message
// copies are what set the slope.
//
// Covers, at each peer count:
// - becomeLeader (builds nextIndex/matchIndex for every peer)
// - election start (tick into Candidate: one RequestVote send per peer)
// - heartbeat fan-out (leader tick: one AppendEntries per peer, each
//   with a log read for its prev-entry term)

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures becomeLeader() over 2 peers.
static void peer_scaling_lead_2(benchmark::State& state) {
    Rig rig(10, 150, 2);
    const Vector<NodeId> peers = makePeers(2);

    for (auto _ : state) {
        rig.node.becomeLeader(peers);
    }
}
BENCHMARK(peer_scaling_lead_2);

// Measures becomeLeader() over 10 peers.
static void peer_scaling_lead_10(benchmark::State& state) {
    Rig rig(10, 150, 10);
    const Vector<NodeId> peers = makePeers(10);

    for (auto _ : state) {
        rig.node.becomeLeader(peers);
    }
}
BENCHMARK(peer_scaling_lead_10);

// Measures becomeLeader() over 100 peers.
static void peer_scaling_lead_100(benchmark::State& state) {
    Rig rig(10, 150, 100);
    const Vector<NodeId> peers = makePeers(100);

    for (auto _ : state) {
        rig.node.becomeLeader(peers);
    }
}
BENCHMARK(peer_scaling_lead_100);

// Measures one election start with 2 peers: step down, then a 1-tick timeout triggers
// startNewElection() and 2 RequestVote sends.
static void peer_scaling_elect_2(benchmark::State& state) {
    Rig rig(0, 1, 2);

    for (auto _ : state) {
        rig.node.stepDownToFollower();
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(peer_scaling_elect_2);

// Measures one election start with 10 peers: step down, then a 1-tick timeout triggers
// startNewElection() and 10 RequestVote sends.
static void peer_scaling_elect_10(benchmark::State& state) {
    Rig rig(0, 1, 10);

    for (auto _ : state) {
        rig.node.stepDownToFollower();
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(peer_scaling_elect_10);

// Measures one election start with 100 peers: step down, then a 1-tick timeout triggers
// startNewElection() and 100 RequestVote sends.
static void peer_scaling_elect_100(benchmark::State& state) {
    Rig rig(0, 1, 100);

    for (auto _ : state) {
        rig.node.stepDownToFollower();
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(peer_scaling_elect_100);

// Measures one leader heartbeat tick with 2 peers: 2 AppendEntries sends.
static void peer_scaling_beat_2(benchmark::State& state) {
    Rig rig(10, 150, 2, 1);
    rig.node.becomeLeader(makePeers(2));

    for (auto _ : state) {
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(peer_scaling_beat_2);

// Measures one leader heartbeat tick with 10 peers: 10 AppendEntries sends.
static void peer_scaling_beat_10(benchmark::State& state) {
    Rig rig(10, 150, 10, 1);
    rig.node.becomeLeader(makePeers(10));

    for (auto _ : state) {
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(peer_scaling_beat_10);

// Measures one leader heartbeat tick with 100 peers: 100 AppendEntries sends.
static void peer_scaling_beat_100(benchmark::State& state) {
    Rig rig(10, 150, 100, 1);
    rig.node.becomeLeader(makePeers(100));

    for (auto _ : state) {
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(peer_scaling_beat_100);
