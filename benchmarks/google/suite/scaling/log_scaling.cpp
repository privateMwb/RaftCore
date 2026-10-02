// Log Scaling Benchmark Suite
// Measures how leader work grows with the number of uncommitted entries
// (1, 100 and 1000), on a 5-node leader at term 7 whose entries are all
// current-term.
//
// Covers, at each log size:
// - commit scan: a success reply with no majority makes
//   tryAdvanceCommitIndex() walk every uncommitted entry, each with a
//   log read and a lookup across all 4 peers -- expected to grow
//   linearly with the log
// - full resend: a failure reply with nextIndex already at 1 makes the
//   leader re-read and re-send the whole log (range read plus the args
//   copy into the Transport) -- also linear, with a heavier constant

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures a non-committing success reply against 1 uncommitted entries:
// the full downward commit scan.
static void log_scaling_commit_1(benchmark::State& state) {
    Rig rig(1);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = true;
    reply.matchIndex = 1;
    const NodeId peer("node-2");

    for (auto _ : state) {
        rig.raft.handleAppendEntriesReply(peer, reply);
    }
}
BENCHMARK(log_scaling_commit_1);

// Measures a non-committing success reply against 100 uncommitted entries:
// the full downward commit scan.
static void log_scaling_commit_100(benchmark::State& state) {
    Rig rig(100);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = true;
    reply.matchIndex = 100;
    const NodeId peer("node-2");

    for (auto _ : state) {
        rig.raft.handleAppendEntriesReply(peer, reply);
    }
}
BENCHMARK(log_scaling_commit_100);

// Measures a non-committing success reply against 1000 uncommitted entries:
// the full downward commit scan.
static void log_scaling_commit_1000(benchmark::State& state) {
    Rig rig(1000);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = true;
    reply.matchIndex = 1000;
    const NodeId peer("node-2");

    for (auto _ : state) {
        rig.raft.handleAppendEntriesReply(peer, reply);
    }
}
BENCHMARK(log_scaling_commit_1000);

// Measures a failure reply with nextIndex pinned at 1: an immediate
// resend of all 1 entries.
static void log_scaling_resend_1(benchmark::State& state) {
    Rig rig(1);
    rig.node.becomeLeader(makePeers());
    const NodeId peer("node-2");
    rig.node.leaderState().nextIndex[peer] = 1;
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = false;

    for (auto _ : state) {
        rig.raft.handleAppendEntriesReply(peer, reply);
    }
}
BENCHMARK(log_scaling_resend_1);

// Measures a failure reply with nextIndex pinned at 1: an immediate
// resend of all 100 entries.
static void log_scaling_resend_100(benchmark::State& state) {
    Rig rig(100);
    rig.node.becomeLeader(makePeers());
    const NodeId peer("node-2");
    rig.node.leaderState().nextIndex[peer] = 1;
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = false;

    for (auto _ : state) {
        rig.raft.handleAppendEntriesReply(peer, reply);
    }
}
BENCHMARK(log_scaling_resend_100);

// Measures a failure reply with nextIndex pinned at 1: an immediate
// resend of all 1000 entries.
static void log_scaling_resend_1000(benchmark::State& state) {
    Rig rig(1000);
    rig.node.becomeLeader(makePeers());
    const NodeId peer("node-2");
    rig.node.leaderState().nextIndex[peer] = 1;
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = false;

    for (auto _ : state) {
        rig.raft.handleAppendEntriesReply(peer, reply);
    }
}
BENCHMARK(log_scaling_resend_1000);
