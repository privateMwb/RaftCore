// Unit Election Test Suite
// Verifies RaftNode's election behavior in isolation: calling
// handleRequestVote()/handleRequestVoteReply()/tick() directly rather
// than driving a multi-node simulation -- the Integration and
// Concurrency suites cover full round trips and multi-actor timing;
// this file is about each individual rule.
//
// Covers:
// - tick()-driven election timeout: no-op before it elapses, Candidate
//   transition the moment it does
// - vote-granting rules: stale-term rejection, the one-vote-per-term
//   rule (including that a retried request from the SAME candidate
//   still succeeds), and the up-to-date-log election restriction
// - vote counting: reaching majority becomes Leader, and a duplicate
//   reply from the same peer isn't double-counted

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void tick_before_timeout_stays_follower() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport; // "B" is never registered -- unused in this test.
    ScriptedRandomSource random(Vector<int>{5});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1, 50);

    for (int i = 0; i < 4; ++i) {
        CHK(node.tick() == Status::OK);
    }
    CHK(state.role() == Role::Follower);
    CHK(state.currentTerm() == kNoTerm);
}

static void tick_at_timeout_becomes_candidate() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{5});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1, 50);

    for (int i = 0; i < 5; ++i) {
        CHK(node.tick() == Status::OK);
    }
    CHK(state.role() == Role::Candidate);
    CHK(state.currentTerm() == 1);
    CHK(state.votedFor().value() == "A");
}

static void grants_vote_up_to_date() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = "X";

    RequestVoteReply reply;
    CHK(node.handleRequestVote(args, reply) == Status::OK);
    CHK(reply.voteGranted);
    CHK(reply.term == 1);
}

static void rejects_vote_for_stale_term() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    CHK(state.observeTerm(2) == Status::OK); // Fast-forward to term 2 directly.
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs args;
    args.term = 1; // Stale.
    args.candidateId = "X";

    RequestVoteReply reply;
    CHK(node.handleRequestVote(args, reply) == Status::OK);
    CHK(!reply.voteGranted);
    CHK(reply.term == 2);
    CHK(!state.votedFor().has_value()); // Untouched.
}

static void rejects_vote_for_new_candidate() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs argsX;
    argsX.term = 1;
    argsX.candidateId = "X";
    RequestVoteReply replyX;
    CHK(node.handleRequestVote(argsX, replyX) == Status::OK);
    CHK(replyX.voteGranted);

    RequestVoteArgs argsY;
    argsY.term = 1; // Same term.
    argsY.candidateId = "Y";
    RequestVoteReply replyY;
    CHK(node.handleRequestVote(argsY, replyY) == Status::OK);
    CHK(!replyY.voteGranted);
    CHK(state.votedFor().value() == "X"); // Unchanged.
}

static void grants_vote_to_same_candidate() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = "X";

    RequestVoteReply reply1;
    CHK(node.handleRequestVote(args, reply1) == Status::OK);
    CHK(reply1.voteGranted);

    RequestVoteReply reply2; // A retried/duplicate RPC from the same candidate.
    CHK(node.handleRequestVote(args, reply2) == Status::OK);
    CHK(reply2.voteGranted); // "votedFor is null OR candidateId" -- same candidate is fine.
}

static void rejects_candidate_with_stale_log() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    LogIndex idx;
    CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK); // lastIndex 2, lastTerm 1.
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = "X";
    args.lastLogIndex = 1; // Behind this follower's index 2.
    args.lastLogTerm = 1;  // Same term, so index is the tiebreaker.

    RequestVoteReply reply;
    CHK(node.handleRequestVote(args, reply) == Status::OK);
    CHK(!reply.voteGranted);
}

static void grants_candidate_with_equal_log() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    LogIndex idx;
    CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = "X";
    args.lastLogIndex = 2; // Exactly matches.
    args.lastLogTerm = 1;

    RequestVoteReply reply;
    CHK(node.handleRequestVote(args, reply) == Status::OK);
    CHK(reply.voteGranted);
}

static void votes_reach_majority_becomes_leader() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport; // Unused -- replies are driven by hand below.
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B", "C", "D"}, 1, 1, 50);

    // NodeState::startElection() directly, bypassing RaftNode's own
    // startNewElection() (which would also send RequestVotes) -- fine
    // here, since this test is about reply handling, not sending.
    CHK(state.startElection() == Status::OK);

    RequestVoteReply replyB;
    replyB.term = 1;
    replyB.voteGranted = true;
    node.handleRequestVoteReply("B", replyB);
    CHK(state.role() == Role::Candidate); // 2 of 4 (self + B) -- majority is 3.

    node.handleRequestVoteReply("B", replyB); // A retried/duplicate reply.
    CHK(state.role() == Role::Candidate);     // Still 2 -- not double-counted.

    RequestVoteReply replyC;
    replyC.term = 1;
    replyC.voteGranted = true;
    node.handleRequestVoteReply("C", replyC);
    CHK(state.role() == Role::Leader); // 3 of 4 (self + B + C) -- majority reached.
}

static void run_tests() {
    RUN(tick_before_timeout_stays_follower);
    RUN(tick_at_timeout_becomes_candidate);
    RUN(grants_vote_up_to_date);
    RUN(rejects_vote_for_stale_term);
    RUN(rejects_vote_for_new_candidate);
    RUN(grants_vote_to_same_candidate);
    RUN(rejects_candidate_with_stale_log);
    RUN(grants_candidate_with_equal_log);
    RUN(votes_reach_majority_becomes_leader);
}

REGISTER_TEST_SUITE();
