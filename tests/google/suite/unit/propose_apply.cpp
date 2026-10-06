// Unit Propose / Apply Test Suite (GoogleTest)
// Verifies RaftNode's client-facing surface in isolation: propose()'s
// rejection rules and applyCommitted()'s delivery rules, driven by
// direct calls rather than a multi-node tick()/pump() choreography.
//
// Covers:
// - propose() on a non-Leader returns NOT_LEADER and appends nothing
// - propose() rejects an empty payload (reserved for the election no-op)
// - applyCommitted() with no StateMachine bound is a no-op
// - applyCommitted() delivers committed entries in order, skips the
//   election no-op, and never re-delivers an already-applied index
// - a StateMachine bound after entries committed receives them on the
//   next applyCommitted() call

#include <gtest/gtest.h>
#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

TEST(ProposeApplyTest, ProposeOnFollowerRejected) {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B", "C"}, 1, 1, 1);

    LogIndex index = 99;
    EXPECT_EQ(node.propose(makePayload("PUT a 1"), index), Status::NOT_LEADER);
    EXPECT_EQ(index, 99); // untouched on failure.
    EXPECT_EQ(storage.lastIndex(), kNoIndex);
}

TEST(ProposeApplyTest, ProposeRejectsEmptyPayload) {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B", "C"}, 1, 1, 1);

    state.becomeLeader(Vector<NodeId>{"B", "C"});

    LogIndex index = 99;
    EXPECT_EQ(node.propose(Vector<std::uint8_t>{}, index), Status::INVALID_ARGUMENT);
    EXPECT_EQ(index, 99);
    EXPECT_EQ(storage.lastIndex(), kNoIndex);
}

TEST(ProposeApplyTest, ApplyWithoutStateMachine) {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);

    LogIndex index;
    EXPECT_EQ(storage.append(1, makePayload("x"), index), Status::OK);
    state.setCommitIndex(1);

    EXPECT_EQ(node.applyCommitted(), Status::OK);
    EXPECT_EQ(state.lastApplied(), kNoIndex); // nothing consumed it.
}

TEST(ProposeApplyTest, ApplyInOrderSkipsNoop) {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);
    RecordingStateMachine machine;
    node.setStateMachine(&machine);

    LogIndex index;
    EXPECT_EQ(storage.append(1, Vector<std::uint8_t>{}, index), Status::OK); // election no-op.
    EXPECT_EQ(storage.append(1, makePayload("first"), index), Status::OK);
    EXPECT_EQ(storage.append(1, makePayload("second"), index), Status::OK);
    EXPECT_EQ(storage.append(1, makePayload("third"), index), Status::OK);

    state.setCommitIndex(3); // first two committed; "third" is not.
    EXPECT_EQ(node.applyCommitted(), Status::OK);

    ASSERT_EQ(machine.indices.size(), 2);
    EXPECT_EQ(machine.indices[0], 2);
    EXPECT_EQ(machine.indices[1], 3);
    EXPECT_EQ(machine.payloads[0], "first");
    EXPECT_EQ(machine.payloads[1], "second");
    EXPECT_EQ(state.lastApplied(), 3); // the no-op's index was consumed too.

    // Idempotent until commitIndex moves again.
    EXPECT_EQ(node.applyCommitted(), Status::OK);
    ASSERT_EQ(machine.indices.size(), 2);

    state.setCommitIndex(4);
    EXPECT_EQ(node.applyCommitted(), Status::OK);
    ASSERT_EQ(machine.indices.size(), 3);
    EXPECT_EQ(machine.indices[2], 4);
    EXPECT_EQ(machine.payloads[2], "third");
}

TEST(ProposeApplyTest, LateBoundMachineCatchesUp) {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);

    LogIndex index;
    EXPECT_EQ(storage.append(1, makePayload("a"), index), Status::OK);
    EXPECT_EQ(storage.append(1, makePayload("b"), index), Status::OK);
    state.setCommitIndex(2);
    EXPECT_EQ(node.applyCommitted(), Status::OK); // unbound: nothing happens.

    RecordingStateMachine machine;
    node.setStateMachine(&machine);
    EXPECT_EQ(node.applyCommitted(), Status::OK);

    ASSERT_EQ(machine.indices.size(), 2);
    EXPECT_EQ(machine.payloads[0], "a");
    EXPECT_EQ(machine.payloads[1], "b");
}
