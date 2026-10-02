// Leader Replication Benchmark Suite
// Measures the leader handling AppendEntries replies: bookkeeping,
// commitIndex advancement, and the immediate resend after a failure.
// A 5-node leader at term 7 with a 10-entry log, all entries current-term.
//
// Covers:
// - stale reply (wrong term, ignored)
// - success reply that can't commit (scans the whole uncommitted log)
// - success reply that commits its entry on the first candidate
// - failure reply (nextIndex back-off + immediate replicateTo resend)

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures a reply from an older term -- observeTerm no-op, then dropped
// on the term check.
static void bench_reply_stale() {
    Rig rig(10);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 3;
    reply.success = true;
    reply.matchIndex = 10;
    const NodeId peer("node-2");

    auto stale = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("reply stale", stale);
}

// Measures a success reply when no majority exists (only node-2 has
// replicated). tryAdvanceCommitIndex() scans all 10 uncommitted entries,
// each with a log read and a check across all 4 peers, and commits none.
static void bench_reply_nocommit() {
    Rig rig(10);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = true;
    reply.matchIndex = 10;
    const NodeId peer("node-2");

    auto noCommit = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("reply nocommit", noCommit);
}

// Measures a success reply that commits. node-3 and node-4 are primed
// with a far-ahead matchIndex, so each newly appended entry has a
// majority the moment node-2 acknowledges it: one candidate scanned,
// commitIndex advances by one. The fake-storage append that creates the
// entry is inside the timed call (one vector push_back).
static void bench_reply_commit() {
    Rig rig(10);
    rig.node.becomeLeader(makePeers());

    AppendEntriesReply ahead;
    ahead.term = 7;
    ahead.success = true;
    ahead.matchIndex = 1000000000;
    rig.raft.handleAppendEntriesReply(NodeId("node-3"), ahead);
    rig.raft.handleAppendEntriesReply(NodeId("node-4"), ahead);

    const NodeId peer("node-2");

    auto commit = [&] {
        LogIndex index;
        Status s = rig.st.append(7, Vector<std::uint8_t>{}, index);
        doNotOptimize(s);

        AppendEntriesReply reply;
        reply.term = 7;
        reply.success = true;
        reply.matchIndex = index;
        rig.raft.handleAppendEntriesReply(peer, reply);
    };
    BENCH_SOLO("reply commit", commit);
}

// Measures a failure reply: nextIndex backs off by one and the leader
// immediately resends. nextIndex reaches 1 within the first 10 calls and
// stays there, so the steady state measured is a full 10-entry resend
// (log range read plus args copy into the transport).
static void bench_reply_back() {
    Rig rig(10);
    rig.node.becomeLeader(makePeers());
    AppendEntriesReply reply;
    reply.term = 7;
    reply.success = false;
    const NodeId peer("node-2");

    auto back = [&] { rig.raft.handleAppendEntriesReply(peer, reply); };
    BENCH_SOLO("reply back", back);
}

// Executes all leader replication benchmark cases.
static void run_benchmarks() {
    bench_reply_stale();
    bench_reply_nocommit();
    bench_reply_commit();
    bench_reply_back();
}

REGISTER_BENCH_SUITE();
