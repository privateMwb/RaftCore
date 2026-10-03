// Integration Restart Recovery Test Suite (GoogleTest)
// Verifies that a node's persisted state (currentTerm, votedFor, and
// the log) survives a simulated restart -- a fresh NodeState/RaftNode
// built over the SAME Storage/PersistentState instances must correctly
// resume exactly where the previous instance left off, honoring an
// already-cast vote and an existing log rather than starting blank.
//
// Covers:
// - currentTerm, votedFor, and the log are all intact immediately after
//   "restart" (constructing new NodeState/RaftNode objects over the
//   same underlying Storage/PersistentState) -- role is NOT persisted
//   and correctly starts back at Follower regardless of what it was
//   before
// - the recovered vote is honored: a different candidate for the same
//   term is rejected, but a retry from the SAME already-voted-for
//   candidate still succeeds
// - a higher term arriving after restart still correctly adopts and
//   grants, exactly as it would have before the restart

#include <gtest/gtest.h>
#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

TEST(RestartRecoveryTest, RestartPreservesTermVoteAndLog) {
    InMemoryStorage storageF;
    InMemoryPersistentState persistF;

    // Seed state as if a prior process already ran for a while: two
    // entries logged, and a vote already cast in term 1.
    LogIndex idx;
    ASSERT_EQ(storageF.append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    ASSERT_EQ(storageF.append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    ASSERT_EQ(persistF.setCurrentTerm(1), Status::OK);
    ASSERT_EQ(persistF.setVotedFor(NodeId("SomeOtherCandidate")), Status::OK);

    // "Restart": brand new NodeState/RaftNode, same underlying storage
    // and persistent state -- nothing here re-derives anything, it's
    // exactly what a real process reopening its files would construct.
    SimulatedTransport transport;
    ScriptedRandomSource randomF(Vector<int>{1000, 1000});
    NodeState stateF(persistF, storageF, "F");
    RaftNode nodeF(stateF, transport, randomF, Vector<NodeId>{"SomeOtherCandidate", "NewCandidate"},
                   1, 1, 50);

    EXPECT_EQ(stateF.currentTerm(), 1);
    ASSERT_TRUE(stateF.votedFor().has_value());
    EXPECT_EQ(stateF.votedFor().value(), "SomeOtherCandidate");
    EXPECT_EQ(stateF.log().lastIndex(), 2);
    EXPECT_EQ(stateF.role(), Role::Follower); // Role isn't persisted -- always starts back here.

    // A different candidate for the SAME term is correctly rejected:
    // proves the recovered vote is actually being honored, not silently
    // reset by the restart.
    RequestVoteArgs otherCandidateArgs;
    otherCandidateArgs.term = 1;
    otherCandidateArgs.candidateId = "NewCandidate";
    otherCandidateArgs.lastLogIndex = 2;
    otherCandidateArgs.lastLogTerm = 1;
    RequestVoteReply reply1;
    EXPECT_EQ(nodeF.handleRequestVote(otherCandidateArgs, reply1), Status::OK);
    EXPECT_FALSE(reply1.voteGranted);

    // A retry from the SAME candidate it already voted for, still in
    // term 1, is fine -- granting again to the same candidate is
    // explicitly allowed ("votedFor is null OR candidateId").
    RequestVoteArgs sameCandidateArgs;
    sameCandidateArgs.term = 1;
    sameCandidateArgs.candidateId = "SomeOtherCandidate";
    sameCandidateArgs.lastLogIndex = 2;
    sameCandidateArgs.lastLogTerm = 1;
    RequestVoteReply reply2;
    EXPECT_EQ(nodeF.handleRequestVote(sameCandidateArgs, reply2), Status::OK);
    EXPECT_TRUE(reply2.voteGranted);

    // A higher term still correctly adopts and grants, exactly as it
    // would have before the restart.
    RequestVoteArgs higherTermArgs;
    higherTermArgs.term = 2;
    higherTermArgs.candidateId = "NewCandidate";
    higherTermArgs.lastLogIndex = 2;
    higherTermArgs.lastLogTerm = 1;
    RequestVoteReply reply3;
    EXPECT_EQ(nodeF.handleRequestVote(higherTermArgs, reply3), Status::OK);
    EXPECT_TRUE(reply3.voteGranted);
    EXPECT_EQ(stateF.currentTerm(), 2);
}
