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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures becomeLeader() over 2 peers.
static void bench_lead_2() {
    Rig rig(10, 150, 2);
    const Vector<NodeId> peers = makePeers(2);

    auto lead = [&] { rig.node.becomeLeader(peers); };
    BENCH_SOLO("lead 2", lead);
}

// Measures becomeLeader() over 10 peers.
static void bench_lead_10() {
    Rig rig(10, 150, 10);
    const Vector<NodeId> peers = makePeers(10);

    auto lead = [&] { rig.node.becomeLeader(peers); };
    BENCH_SOLO("lead 10", lead);
}

// Measures becomeLeader() over 100 peers.
static void bench_lead_100() {
    Rig rig(10, 150, 100);
    const Vector<NodeId> peers = makePeers(100);

    auto lead = [&] { rig.node.becomeLeader(peers); };
    BENCH_SOLO("lead 100", lead);
}

// Measures one election start with 2 peers: step down, then a 1-tick timeout triggers
// startNewElection() and 2 RequestVote sends.
static void bench_elect_2() {
    Rig rig(0, 1, 2);

    auto elect = [&] {
        rig.node.stepDownToFollower();
        Status s = rig.raft.tick();
        doNotOptimize(s);
    };
    BENCH_SOLO("elect 2", elect);
}

// Measures one election start with 10 peers: step down, then a 1-tick timeout triggers
// startNewElection() and 10 RequestVote sends.
static void bench_elect_10() {
    Rig rig(0, 1, 10);

    auto elect = [&] {
        rig.node.stepDownToFollower();
        Status s = rig.raft.tick();
        doNotOptimize(s);
    };
    BENCH_SOLO("elect 10", elect);
}

// Measures one election start with 100 peers: step down, then a 1-tick timeout triggers
// startNewElection() and 100 RequestVote sends.
static void bench_elect_100() {
    Rig rig(0, 1, 100);

    auto elect = [&] {
        rig.node.stepDownToFollower();
        Status s = rig.raft.tick();
        doNotOptimize(s);
    };
    BENCH_SOLO("elect 100", elect);
}

// Measures one leader heartbeat tick with 2 peers: 2 AppendEntries sends.
static void bench_beat_2() {
    Rig rig(10, 150, 2, 1);
    rig.node.becomeLeader(makePeers(2));

    auto beat = [&] {
        Status s = rig.raft.tick();
        doNotOptimize(s);
    };
    BENCH_SOLO("beat 2", beat);
}

// Measures one leader heartbeat tick with 10 peers: 10 AppendEntries sends.
static void bench_beat_10() {
    Rig rig(10, 150, 10, 1);
    rig.node.becomeLeader(makePeers(10));

    auto beat = [&] {
        Status s = rig.raft.tick();
        doNotOptimize(s);
    };
    BENCH_SOLO("beat 10", beat);
}

// Measures one leader heartbeat tick with 100 peers: 100 AppendEntries sends.
static void bench_beat_100() {
    Rig rig(10, 150, 100, 1);
    rig.node.becomeLeader(makePeers(100));

    auto beat = [&] {
        Status s = rig.raft.tick();
        doNotOptimize(s);
    };
    BENCH_SOLO("beat 100", beat);
}

// Executes all peer-scaling benchmark cases.
static void run_benchmarks() {
    bench_lead_2();
    bench_lead_10();
    bench_lead_100();
    bench_elect_2();
    bench_elect_10();
    bench_elect_100();
    bench_beat_2();
    bench_beat_10();
    bench_beat_100();
}

REGISTER_BENCH_SUITE();
