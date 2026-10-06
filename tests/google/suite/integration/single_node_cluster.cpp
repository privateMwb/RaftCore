// Integration Single Node Cluster Test Suite (GoogleTest)
// Verifies a cluster of exactly one RaftNode (no peers, no network):
// it must elect itself, commit on its own, and apply client commands
// synchronously -- the shape the demo app's first, no-networking phase
// runs in.
//
// Covers:
// - a lone node becomes Leader on its first election timeout, with no
//   RequestVote replies involved
// - its election no-op commits without any peer ack
// - propose() commits and applies inside the call, in order
// - a restarted node (same Storage/PersistentState, fresh state machine)
//   re-elects itself and replays every committed command from index 1

#include <gtest/gtest.h>
#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

TEST(SingleNodeClusterTest, LoneNodeElectsItself) {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1, 1, 1});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);

    EXPECT_EQ(state.role(), Role::Follower);
    EXPECT_EQ(node.tick(), Status::OK);

    EXPECT_EQ(state.role(), Role::Leader);
    EXPECT_EQ(state.currentTerm(), 1);
    EXPECT_EQ(storage.lastIndex(), 1); // the election no-op.
    EXPECT_EQ(state.commitIndex(), 1); // committed with no peer involved.
}

TEST(SingleNodeClusterTest, ProposeCommitsAndApplies) {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1, 1, 1});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);
    RecordingStateMachine machine;
    node.setStateMachine(&machine);

    EXPECT_EQ(node.tick(), Status::OK);

    LogIndex first = 0, second = 0;
    EXPECT_EQ(node.propose(makePayload("PUT a 1"), first), Status::OK);
    EXPECT_EQ(node.propose(makePayload("PUT b 2"), second), Status::OK);

    EXPECT_EQ(first, 2);
    EXPECT_EQ(second, 3);
    EXPECT_EQ(state.commitIndex(), 3);
    EXPECT_EQ(state.lastApplied(), 3);

    // Applied inside propose() itself -- no further tick() needed.
    ASSERT_EQ(machine.indices.size(), 2);
    EXPECT_EQ(machine.indices[0], 2);
    EXPECT_EQ(machine.indices[1], 3);
    EXPECT_EQ(machine.payloads[0], "PUT a 1");
    EXPECT_EQ(machine.payloads[1], "PUT b 2");
}

TEST(SingleNodeClusterTest, RestartReplaysFromStart) {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;

    {
        NodeState state(persist, storage, "A");
        ScriptedRandomSource random(Vector<int>{1, 1, 1});
        RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);
        RecordingStateMachine machine;
        node.setStateMachine(&machine);

        EXPECT_EQ(node.tick(), Status::OK);
        LogIndex index;
        EXPECT_EQ(node.propose(makePayload("PUT a 1"), index), Status::OK);
        EXPECT_EQ(node.propose(makePayload("DEL a"), index), Status::OK);
        ASSERT_EQ(machine.indices.size(), 2);
    } // "crash": volatile state (role, commitIndex, lastApplied) is gone.

    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1, 1, 1});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);
    RecordingStateMachine rebuilt;
    node.setStateMachine(&rebuilt);

    EXPECT_EQ(state.role(), Role::Follower);
    EXPECT_EQ(state.commitIndex(), kNoIndex);
    EXPECT_EQ(rebuilt.indices.size(), 0);

    EXPECT_EQ(node.tick(), Status::OK); // re-elects itself in term 2.

    EXPECT_EQ(state.role(), Role::Leader);
    EXPECT_EQ(state.currentTerm(), 2);
    // New term's no-op at index 4 commits, which drags indices 1-3 with it.
    EXPECT_EQ(state.commitIndex(), 4);
    ASSERT_EQ(rebuilt.indices.size(), 2);
    EXPECT_EQ(rebuilt.payloads[0], "PUT a 1");
    EXPECT_EQ(rebuilt.payloads[1], "DEL a");
}
