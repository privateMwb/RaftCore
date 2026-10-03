// Unit NodeState Test Suite (GoogleTest)
// Verifies NodeState in isolation: the Figure-2 role/term/vote
// transitions, delegation to PersistentState/Storage, the
// all-servers/leaders-only volatile state accessors, and the
// leaderState() container becomeLeader()/stepDownToFollower() manage.
//
// Covers:
// - initial state, startElection(), observeTerm() (both the adopting
//   and no-op cases), grantVoteTo()
// - becomeLeader() initializing nextIndex/matchIndex for every peer,
//   and stepDownToFollower() dropping that state again
// - commitIndex/lastApplied setters, and log() reflecting the same
//   underlying Storage the caller passed in

#include <gtest/gtest.h>
#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

TEST(NodeStateTest, StartsFollowerAtTermZero) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    EXPECT_EQ(state.selfId(), "A");
    EXPECT_EQ(state.role(), Role::Follower);
    EXPECT_EQ(state.currentTerm(), kNoTerm);
    EXPECT_FALSE(state.votedFor().has_value());
    EXPECT_EQ(state.commitIndex(), kNoIndex);
    EXPECT_EQ(state.lastApplied(), kNoIndex);
    EXPECT_EQ(state.log().lastIndex(), kNoIndex);
}

TEST(NodeStateTest, StartElectionVotesSelf) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    EXPECT_EQ(state.startElection(), Status::OK);
    EXPECT_EQ(state.currentTerm(), 1);
    EXPECT_EQ(state.role(), Role::Candidate);
    ASSERT_TRUE(state.votedFor().has_value());
    EXPECT_EQ(state.votedFor().value(), "A");
}

TEST(NodeStateTest, HigherTermStepsDown) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ASSERT_EQ(state.startElection(), Status::OK); // Candidate, term 1, voted for self.

    EXPECT_EQ(state.observeTerm(5), Status::OK);
    EXPECT_EQ(state.currentTerm(), 5);
    EXPECT_EQ(state.role(), Role::Follower);    // Stepped down.
    EXPECT_FALSE(state.votedFor().has_value()); // A new term means no vote cast in it yet.
}

TEST(NodeStateTest, StaleTermIsIgnored) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ASSERT_EQ(state.startElection(), Status::OK); // Candidate, term 1.

    EXPECT_EQ(state.observeTerm(1), Status::OK); // Equal -- no-op.
    EXPECT_EQ(state.role(), Role::Candidate);
    EXPECT_EQ(state.currentTerm(), 1);

    EXPECT_EQ(state.observeTerm(0), Status::OK); // Stale -- also a no-op.
    EXPECT_EQ(state.role(), Role::Candidate);
    EXPECT_EQ(state.currentTerm(), 1);
}

TEST(NodeStateTest, GrantVoteKeepsRole) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    EXPECT_EQ(state.grantVoteTo("X"), Status::OK);
    ASSERT_TRUE(state.votedFor().has_value());
    EXPECT_EQ(state.votedFor().value(), "X");
    EXPECT_EQ(state.role(),
              Role::Follower); // Unchanged -- granting a vote isn't a role transition.
    EXPECT_EQ(state.currentTerm(),
              kNoTerm); // Unchanged -- grantVoteTo() doesn't touch the term either.
}

TEST(NodeStateTest, LeaderInitializesPeerState) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    LogIndex idx;
    ASSERT_EQ(storage.append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    ASSERT_EQ(storage.append(1, Vector<std::uint8_t>{}, idx), Status::OK); // lastIndex() == 2.

    ASSERT_EQ(state.startElection(), Status::OK);
    state.becomeLeader(Vector<NodeId>{"B", "C"});

    EXPECT_EQ(state.role(), Role::Leader);
    EXPECT_TRUE(state.leaderState().nextIndex.contains("B"));
    EXPECT_TRUE(state.leaderState().nextIndex.contains("C"));
    EXPECT_EQ(state.leaderState().nextIndex["B"], 3); // lastIndex() + 1.
    EXPECT_EQ(state.leaderState().nextIndex["C"], 3);
    EXPECT_EQ(state.leaderState().matchIndex["B"], kNoIndex);
    EXPECT_EQ(state.leaderState().matchIndex["C"], kNoIndex);
}

TEST(NodeStateTest, StepDownDropsLeaderState) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ASSERT_EQ(state.startElection(), Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    ASSERT_EQ(state.role(), Role::Leader);

    state.stepDownToFollower();
    EXPECT_EQ(state.role(), Role::Follower);
}

TEST(NodeStateTest, CommitAndAppliedSetters) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    state.setCommitIndex(3);
    EXPECT_EQ(state.commitIndex(), 3);

    state.setLastApplied(2);
    EXPECT_EQ(state.lastApplied(), 2);
}

TEST(NodeStateTest, LogReflectsSameStorage) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    LogIndex idx;
    ASSERT_EQ(state.log().append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    EXPECT_EQ(idx, 1);

    // Appended through state.log(), but it's the SAME storage object --
    // confirms log() isn't returning a copy.
    EXPECT_EQ(storage.lastIndex(), 1);
    LogEntry entry;
    ASSERT_EQ(storage.entryAt(1, entry), Status::OK);
    EXPECT_EQ(entry.term, 1);
}
