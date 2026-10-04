// Lifecycle Term Monotonicity Test Suite (GoogleTest)
// Verifies that currentTerm and commitIndex never move backward,
// exercised across more than one of the methods that touch them -- not
// just a single call site repeated, but the invariant holding as a
// property of the whole object as it's driven through several of its
// transition paths.
//
// Covers:
// - currentTerm ignoring both a stale (lower) and an equal term
// - currentTerm only ever rising, across repeated elections
// - commitIndex on a Leader never regressing even when a delayed,
//   out-of-order AppendEntriesReply reports a lower matchIndex than one
//   already seen

#include <gtest/gtest.h>
#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

TEST(TermMonotonicityTest, TermNeverDecreasesOnStale) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    EXPECT_EQ(state.observeTerm(5), Status::OK);
    EXPECT_EQ(state.currentTerm(), 5);

    EXPECT_EQ(state.observeTerm(2), Status::OK); // Stale -- ignored.
    EXPECT_EQ(state.currentTerm(), 5);

    EXPECT_EQ(state.observeTerm(5), Status::OK); // Equal -- also ignored.
    EXPECT_EQ(state.currentTerm(), 5);
}

TEST(TermMonotonicityTest, TermOnlyRisesThroughElections) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    Term previous = state.currentTerm();
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(state.startElection(), Status::OK);
        EXPECT_GT(state.currentTerm(), previous); // Strictly greater each time.
        previous = state.currentTerm();
    }
    EXPECT_EQ(state.currentTerm(), 3);
}

TEST(TermMonotonicityTest, CommitIndexNeverRegressesLeader) {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "L");

    ASSERT_EQ(state.startElection(),
              Status::OK); // Term 1, so entries below can legitimately be committed.
    state.becomeLeader(Vector<NodeId>{"B"});

    LogIndex idx;
    for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(storage.append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    }

    SimulatedTransport transport; // Unused -- replies are driven by hand below.
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1, 50);

    AppendEntriesReply highReply;
    highReply.term = state.currentTerm();
    highReply.success = true;
    highReply.matchIndex = 3;
    node.handleAppendEntriesReply("B", highReply);
    ASSERT_EQ(state.commitIndex(), 3); // self + B reaches this 2-node cluster's majority.

    // A delayed or reordered reply arrives afterward, reporting a LOWER
    // matchIndex than one already seen for the same peer.
    AppendEntriesReply lowReply;
    lowReply.term = state.currentTerm();
    lowReply.success = true;
    lowReply.matchIndex = 1;
    node.handleAppendEntriesReply("B", lowReply);
    EXPECT_EQ(state.commitIndex(), 3); // Unchanged -- never regresses.
}
