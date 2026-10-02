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

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Measures handleRequestVote() from a candidate with an older term.
static void request_vote_rv_stale(benchmark::State& state) {
    Rig rig;
    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = "node-2";
    RequestVoteReply reply;

    for (auto _ : state) {
        Status s = rig.raft.handleRequestVote(args, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(request_vote_rv_stale);

// Measures handleRequestVote() granting a vote. The same candidate can
// be granted repeatedly in one term, so every call takes the full grant
// path: log check, setVotedFor, timer reset.
static void request_vote_rv_grant(benchmark::State& state) {
    Rig rig;
    RequestVoteArgs args;
    args.term = 7;
    args.candidateId = "node-2";
    RequestVoteReply reply;

    for (auto _ : state) {
        Status s = rig.raft.handleRequestVote(args, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(request_vote_rv_grant);

// Measures handleRequestVote() denying a candidate because this node
// already voted for a different one this term.
static void request_vote_rv_deny(benchmark::State& state) {
    Rig rig;
    (void)rig.ps.setVotedFor(NodeId("node-3"));
    RequestVoteArgs args;
    args.term = 7;
    args.candidateId = "node-2";
    RequestVoteReply reply;

    for (auto _ : state) {
        Status s = rig.raft.handleRequestVote(args, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(request_vote_rv_deny);

// Measures handleRequestVoteReply() on a node that isn't a Candidate --
// the early-out for late replies.
static void request_vote_rvr_ignore(benchmark::State& state) {
    Rig rig;
    RequestVoteReply reply;
    reply.term = 7;
    reply.voteGranted = true;
    const NodeId peer("node-2");

    for (auto _ : state) {
        rig.raft.handleRequestVoteReply(peer, reply);
    }
}
BENCHMARK(request_vote_rvr_ignore);

// Measures handleRequestVoteReply() for a peer already counted this
// election (a retried RPC): term check, role check, then the
// votesReceivedFrom_ lookup.
static void request_vote_rvr_dup(benchmark::State& state) {
    Rig rig(0, 1);
    (void)rig.raft.tick(); // 1-tick timeout: becomes Candidate at term 8.
    RequestVoteReply reply;
    reply.term = rig.node.currentTerm();
    reply.voteGranted = true;
    const NodeId peer("node-2");
    rig.raft.handleRequestVoteReply(peer, reply); // Counted once, here.

    for (auto _ : state) {
        rig.raft.handleRequestVoteReply(peer, reply);
    }
}
BENCHMARK(request_vote_rvr_dup);

// Measures one complete election with a 1-tick timeout: step down,
// tick into Candidate (4 RequestVote sends), two granting replies reach
// majority, then becomeLeader + no-op append + 4 AppendEntries sends.
static void request_vote_election_round(benchmark::State& state) {
    Rig rig(0, 1);
    const NodeId a("node-2");
    const NodeId b("node-3");

    for (auto _ : state) {
        rig.node.stepDownToFollower();
        Status s = rig.raft.tick();
        benchmark::DoNotOptimize(s);

        RequestVoteReply reply;
        reply.term = rig.node.currentTerm();
        reply.voteGranted = true;
        rig.raft.handleRequestVoteReply(a, reply);
        rig.raft.handleRequestVoteReply(b, reply);
    }
}
BENCHMARK(request_vote_election_round);
