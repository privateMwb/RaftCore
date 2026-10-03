// Unit Election Test Suite (GoogleTest)
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

#include <gtest/gtest.h>
#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

TEST(ElectionTest, TickBeforeTimeoutStaysFollower) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport; // "B" is never registered -- unused in this test.
    ScriptedRandomSource random(Vector<int>{5});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1, 50);

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(node.tick(), Status::OK);
    }
    EXPECT_EQ(state.role(), Role::Follower);
    EXPECT_EQ(state.currentTerm(), kNoTerm);
}

TEST(ElectionTest, TickAtTimeoutBecomesCandidate) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{5});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1, 50);

    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(node.tick(), Status::OK);
    }
    EXPECT_EQ(state.role(), Role::Candidate);
    EXPECT_EQ(state.currentTerm(), 1);
    ASSERT_TRUE(state.votedFor().has_value());
    EXPECT_EQ(state.votedFor().value(), "A");
}

TEST(ElectionTest, GrantsVoteUpToDate) {
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
    EXPECT_EQ(node.handleRequestVote(args, reply), Status::OK);
    EXPECT_TRUE(reply.voteGranted);
    EXPECT_EQ(reply.term, 1);
}

TEST(ElectionTest, RejectsVoteForStaleTerm) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ASSERT_EQ(state.observeTerm(2), Status::OK); // Fast-forward to term 2 directly.
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs args;
    args.term = 1; // Stale.
    args.candidateId = "X";

    RequestVoteReply reply;
    EXPECT_EQ(node.handleRequestVote(args, reply), Status::OK);
    EXPECT_FALSE(reply.voteGranted);
    EXPECT_EQ(reply.term, 2);
    EXPECT_FALSE(state.votedFor().has_value()); // Untouched.
}

TEST(ElectionTest, RejectsVoteForNewCandidate) {
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
    ASSERT_EQ(node.handleRequestVote(argsX, replyX), Status::OK);
    ASSERT_TRUE(replyX.voteGranted);

    RequestVoteArgs argsY;
    argsY.term = 1; // Same term.
    argsY.candidateId = "Y";
    RequestVoteReply replyY;
    EXPECT_EQ(node.handleRequestVote(argsY, replyY), Status::OK);
    EXPECT_FALSE(replyY.voteGranted);
    ASSERT_TRUE(state.votedFor().has_value());
    EXPECT_EQ(state.votedFor().value(), "X"); // Unchanged.
}

TEST(ElectionTest, GrantsVoteToSameCandidate) {
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
    ASSERT_EQ(node.handleRequestVote(args, reply1), Status::OK);
    ASSERT_TRUE(reply1.voteGranted);

    RequestVoteReply reply2; // A retried/duplicate RPC from the same candidate.
    EXPECT_EQ(node.handleRequestVote(args, reply2), Status::OK);
    EXPECT_TRUE(reply2.voteGranted); // "votedFor is null OR candidateId" -- same candidate is fine.
}

TEST(ElectionTest, RejectsCandidateWithStaleLog) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    LogIndex idx;
    ASSERT_EQ(storage.append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    ASSERT_EQ(storage.append(1, Vector<std::uint8_t>{}, idx), Status::OK); // lastIndex 2, lastTerm 1.
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = "X";
    args.lastLogIndex = 1; // Behind this follower's index 2.
    args.lastLogTerm = 1;  // Same term, so index is the tiebreaker.

    RequestVoteReply reply;
    EXPECT_EQ(node.handleRequestVote(args, reply), Status::OK);
    EXPECT_FALSE(reply.voteGranted);
}

TEST(ElectionTest, GrantsCandidateWithEqualLog) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    LogIndex idx;
    ASSERT_EQ(storage.append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    ASSERT_EQ(storage.append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = "X";
    args.lastLogIndex = 2; // Exactly matches.
    args.lastLogTerm = 1;

    RequestVoteReply reply;
    EXPECT_EQ(node.handleRequestVote(args, reply), Status::OK);
    EXPECT_TRUE(reply.voteGranted);
}

TEST(ElectionTest, VotesReachMajorityBecomesLeader) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport; // Unused -- replies are driven by hand below.
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B", "C", "D"}, 1, 1, 50);

    // NodeState::startElection() directly, bypassing RaftNode's own
    // startNewElection() (which would also send RequestVotes) -- fine
    // here, since this test is about reply handling, not sending.
    ASSERT_EQ(state.startElection(), Status::OK);

    RequestVoteReply replyB;
    replyB.term = 1;
    replyB.voteGranted = true;
    node.handleRequestVoteReply("B", replyB);
    EXPECT_EQ(state.role(), Role::Candidate); // 2 of 4 (self + B) -- majority is 3.

    node.handleRequestVoteReply("B", replyB); // A retried/duplicate reply.
    EXPECT_EQ(state.role(), Role::Candidate); // Still 2 -- not double-counted.

    RequestVoteReply replyC;
    replyC.term = 1;
    replyC.voteGranted = true;
    node.handleRequestVoteReply("C", replyC);
    EXPECT_EQ(state.role(), Role::Leader); // 3 of 4 (self + B + C) -- majority reached.
}
