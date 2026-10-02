// Integration Restart Recovery Test Suite
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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void restart_preserves_term_vote_and_log() {
    InMemoryStorage storageF;
    InMemoryPersistentState persistF;

    // Seed state as if a prior process already ran for a while: two
    // entries logged, and a vote already cast in term 1.
    LogIndex idx;
    CHK(storageF.append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    CHK(storageF.append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    CHK(persistF.setCurrentTerm(1) == Status::OK);
    CHK(persistF.setVotedFor(NodeId("SomeOtherCandidate")) == Status::OK);

    // "Restart": brand new NodeState/RaftNode, same underlying storage
    // and persistent state -- nothing here re-derives anything, it's
    // exactly what a real process reopening its files would construct.
    SimulatedTransport transport;
    ScriptedRandomSource randomF(Vector<int>{1000, 1000});
    NodeState stateF(persistF, storageF, "F");
    RaftNode nodeF(stateF, transport, randomF, Vector<NodeId>{"SomeOtherCandidate", "NewCandidate"},
                   1, 1, 50);

    CHK(stateF.currentTerm() == 1);
    CHK(stateF.votedFor().has_value());
    CHK(stateF.votedFor().value() == "SomeOtherCandidate");
    CHK(stateF.log().lastIndex() == 2);
    CHK(stateF.role() == Role::Follower); // Role isn't persisted -- always starts back here.

    // A different candidate for the SAME term is correctly rejected:
    // proves the recovered vote is actually being honored, not silently
    // reset by the restart.
    RequestVoteArgs otherCandidateArgs;
    otherCandidateArgs.term = 1;
    otherCandidateArgs.candidateId = "NewCandidate";
    otherCandidateArgs.lastLogIndex = 2;
    otherCandidateArgs.lastLogTerm = 1;
    RequestVoteReply reply1;
    CHK(nodeF.handleRequestVote(otherCandidateArgs, reply1) == Status::OK);
    CHK(!reply1.voteGranted);

    // A retry from the SAME candidate it already voted for, still in
    // term 1, is fine -- granting again to the same candidate is
    // explicitly allowed ("votedFor is null OR candidateId").
    RequestVoteArgs sameCandidateArgs;
    sameCandidateArgs.term = 1;
    sameCandidateArgs.candidateId = "SomeOtherCandidate";
    sameCandidateArgs.lastLogIndex = 2;
    sameCandidateArgs.lastLogTerm = 1;
    RequestVoteReply reply2;
    CHK(nodeF.handleRequestVote(sameCandidateArgs, reply2) == Status::OK);
    CHK(reply2.voteGranted);

    // A higher term still correctly adopts and grants, exactly as it
    // would have before the restart.
    RequestVoteArgs higherTermArgs;
    higherTermArgs.term = 2;
    higherTermArgs.candidateId = "NewCandidate";
    higherTermArgs.lastLogIndex = 2;
    higherTermArgs.lastLogTerm = 1;
    RequestVoteReply reply3;
    CHK(nodeF.handleRequestVote(higherTermArgs, reply3) == Status::OK);
    CHK(reply3.voteGranted);
    CHK(stateF.currentTerm() == 2);
}

static void run_tests() {
    RUN(restart_preserves_term_vote_and_log);
}

REGISTER_TEST_SUITE();
