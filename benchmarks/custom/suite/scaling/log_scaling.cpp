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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures a non-committing success reply against 1 uncommitted entries:
// the full downward commit scan.
static void bench_commit_1() {
    Rig rig(1);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = true;
    reply.matchIndex = 1;
    const NodeId peer("node-2");

    auto scan = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("commit 1", scan);
}

// Measures a non-committing success reply against 100 uncommitted entries:
// the full downward commit scan.
static void bench_commit_100() {
    Rig rig(100);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = true;
    reply.matchIndex = 100;
    const NodeId peer("node-2");

    auto scan = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("commit 100", scan);
}

// Measures a non-committing success reply against 1000 uncommitted entries:
// the full downward commit scan.
static void bench_commit_1000() {
    Rig rig(1000);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = true;
    reply.matchIndex = 1000;
    const NodeId peer("node-2");

    auto scan = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("commit 1000", scan);
}

// Measures a failure reply with nextIndex pinned at 1: an immediate
// resend of all 1 entries.
static void bench_resend_1() {
    Rig rig(1);
    rig.node.becomeLeader(makePeers());
    const NodeId peer("node-2");
    rig.node.leaderState().nextIndex[peer] = 1;
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = false;

    auto resend = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("resend 1", resend);
}

// Measures a failure reply with nextIndex pinned at 1: an immediate
// resend of all 100 entries.
static void bench_resend_100() {
    Rig rig(100);
    rig.node.becomeLeader(makePeers());
    const NodeId peer("node-2");
    rig.node.leaderState().nextIndex[peer] = 1;
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = false;

    auto resend = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("resend 100", resend);
}

// Measures a failure reply with nextIndex pinned at 1: an immediate
// resend of all 1000 entries.
static void bench_resend_1000() {
    Rig rig(1000);
    rig.node.becomeLeader(makePeers());
    const NodeId peer("node-2");
    rig.node.leaderState().nextIndex[peer] = 1;
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = false;

    auto resend = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("resend 1000", resend);
}

// Executes all log-scaling benchmark cases.
static void run_benchmarks() {
    bench_commit_1();
    bench_commit_100();
    bench_commit_1000();
    bench_resend_1();
    bench_resend_100();
    bench_resend_1000();
}

REGISTER_BENCH_SUITE();
