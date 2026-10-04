// Lifecycle Role Transition Rules Test Suite (GoogleTest)
// Verifies that becomeLeader() always initializes fresh leader-only
// state, no matter what happened before it -- a previous leadership
// stint's nextIndex/matchIndex values must never leak into the next
// one, regardless of which transition (a new election, stepping down,
// or observing a newer term) sits between them.
//
// Covers:
// - becomeLeader() called again after already having been Leader before
//   produces fresh values, not leftovers from the earlier stint
// - the same, with stepDownToFollower() as the thing that happened
//   in between
// - the same, with observeTerm() (a newer term arriving) as the thing
//   that happened in between

#include <gtest/gtest.h>
#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

TEST(RoleTransitionRulesTest, BecomeLeaderAlwaysResetsState) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    ASSERT_EQ(state.startElection(), Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    state.leaderState().nextIndex["B"] = 99; // Simulate some leadership progress.
    state.leaderState().matchIndex["B"] = 50;

    // A later election win (a new term) and another becomeLeader() call
    // must not carry any of that over.
    ASSERT_EQ(state.startElection(), Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    EXPECT_EQ(state.leaderState().nextIndex["B"], 1);         // Freshly computed, not 99.
    EXPECT_EQ(state.leaderState().matchIndex["B"], kNoIndex); // Not 50.
}

TEST(RoleTransitionRulesTest, SteppingDownClearsLeaderState) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    ASSERT_EQ(state.startElection(), Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    state.leaderState().nextIndex["B"] = 99;

    state.stepDownToFollower();
    ASSERT_EQ(state.role(), Role::Follower);

    ASSERT_EQ(state.startElection(), Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    EXPECT_EQ(state.leaderState().nextIndex["B"], 1); // Fresh, not 99.
}

TEST(RoleTransitionRulesTest, ObserveTermClearsLeaderState) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    ASSERT_EQ(state.startElection(), Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    state.leaderState().nextIndex["B"] = 99;

    ASSERT_EQ(state.observeTerm(100), Status::OK); // A much higher term arrives.
    ASSERT_EQ(state.role(), Role::Follower);

    ASSERT_EQ(state.startElection(), Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    EXPECT_EQ(state.leaderState().nextIndex["B"], 1); // Fresh, not 99.
}
