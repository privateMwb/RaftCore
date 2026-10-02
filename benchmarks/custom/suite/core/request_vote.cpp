// Request Vote Benchmark Suite
// Measures the RequestVote path on both sides: a node deciding how to
// answer a candidate, and a candidate handling replies up to winning.
//
// Covers:
// - handleRequestVote stale (rejected on term), granted, denied
//   (already voted for someone else)
// - handleRequestVoteReply ignored (not a Candidate) and duplicate
//   (already-counted peer)
// - full election round: timeout -> Candidate -> majority -> Leader
//   (includes the no-op append and first heartbeat fan-out)

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures handleRequestVote() from a candidate with an older term.
static void bench_rv_stale() {
    Rig rig;
    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = "node-2";
    RequestVoteReply reply;

    auto stale = [&] {
        Status s = rig.raft.handleRequestVote(args, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("rv stale", stale);
}

// Measures handleRequestVote() granting a vote. The same candidate can
// be granted repeatedly in one term, so every call takes the full grant
// path: log check, setVotedFor, timer reset.
static void bench_rv_grant() {
    Rig rig;
    RequestVoteArgs args;
    args.term = 7;
    args.candidateId = "node-2";
    RequestVoteReply reply;

    auto grant = [&] {
        Status s = rig.raft.handleRequestVote(args, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("rv grant", grant);
}

// Measures handleRequestVote() denying a candidate because this node
// already voted for a different one this term.
static void bench_rv_deny() {
    Rig rig;
    (void)rig.ps.setVotedFor(NodeId("node-3"));
    RequestVoteArgs args;
    args.term = 7;
    args.candidateId = "node-2";
    RequestVoteReply reply;

    auto deny = [&] {
        Status s = rig.raft.handleRequestVote(args, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("rv deny", deny);
}

// Measures handleRequestVoteReply() on a node that isn't a Candidate --
// the early-out for late replies.
static void bench_rvr_ignore() {
    Rig rig;
    RequestVoteReply reply;
    reply.term = 7;
    reply.voteGranted = true;
    const NodeId peer("node-2");

    auto ignore = [&] { rig.raft.handleRequestVoteReply(peer, reply); };
    BENCH_SOLO("rvr ignore", ignore);
}

// Measures handleRequestVoteReply() for a peer already counted this
// election (a retried RPC): term check, role check, then the
// votesReceivedFrom_ lookup.
static void bench_rvr_dup() {
    Rig rig(0, 1);
    (void)rig.raft.tick(); // 1-tick timeout: becomes Candidate at term 8.
    RequestVoteReply reply;
    reply.term = rig.node.currentTerm();
    reply.voteGranted = true;
    const NodeId peer("node-2");
    rig.raft.handleRequestVoteReply(peer, reply); // Counted once, here.

    auto dup = [&] { rig.raft.handleRequestVoteReply(peer, reply); };
    BENCH_SOLO("rvr dup", dup);
}

// Measures one complete election with a 1-tick timeout: step down,
// tick into Candidate (4 RequestVote sends), two granting replies reach
// majority, then becomeLeader + no-op append + 4 AppendEntries sends.
static void bench_election_round() {
    Rig rig(0, 1);
    const NodeId a("node-2");
    const NodeId b("node-3");

    auto round = [&] {
        rig.node.stepDownToFollower();
        Status s = rig.raft.tick();
        doNotOptimize(s);

        RequestVoteReply reply;
        reply.term = rig.node.currentTerm();
        reply.voteGranted = true;
        rig.raft.handleRequestVoteReply(a, reply);
        rig.raft.handleRequestVoteReply(b, reply);
    };
    BENCH_SOLO("elect round", round);
}

// Executes all RequestVote benchmark cases.
static void run_benchmarks() {
    bench_rv_stale();
    bench_rv_grant();
    bench_rv_deny();
    bench_rvr_ignore();
    bench_rvr_dup();
    bench_election_round();
}

REGISTER_BENCH_SUITE();
