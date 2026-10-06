// Integration Single Node Cluster Test Suite
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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void lone_node_elects_itself() {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1, 1, 1});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);

    CHK(state.role() == Role::Follower);
    CHK(node.tick() == Status::OK);

    CHK(state.role() == Role::Leader);
    CHK(state.currentTerm() == 1);
    CHK(storage.lastIndex() == 1); // the election no-op.
    CHK(state.commitIndex() == 1); // committed with no peer involved.
}

static void propose_commits_and_applies() {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1, 1, 1});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);
    RecordingStateMachine machine;
    node.setStateMachine(&machine);

    CHK(node.tick() == Status::OK);

    LogIndex first = 0, second = 0;
    CHK(node.propose(makePayload("PUT a 1"), first) == Status::OK);
    CHK(node.propose(makePayload("PUT b 2"), second) == Status::OK);

    CHK(first == 2);
    CHK(second == 3);
    CHK(state.commitIndex() == 3);
    CHK(state.lastApplied() == 3);

    // Applied inside propose() itself -- no further tick() needed.
    CHK(machine.indices.size() == 2);
    CHK(machine.indices[0] == 2);
    CHK(machine.indices[1] == 3);
    CHK(machine.payloads[0] == "PUT a 1");
    CHK(machine.payloads[1] == "PUT b 2");
}

static void restart_replays_from_start() {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;

    {
        NodeState state(persist, storage, "A");
        ScriptedRandomSource random(Vector<int>{1, 1, 1});
        RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);
        RecordingStateMachine machine;
        node.setStateMachine(&machine);

        CHK(node.tick() == Status::OK);
        LogIndex index;
        CHK(node.propose(makePayload("PUT a 1"), index) == Status::OK);
        CHK(node.propose(makePayload("DEL a"), index) == Status::OK);
        CHK(machine.indices.size() == 2);
    } // "crash": volatile state (role, commitIndex, lastApplied) is gone.

    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1, 1, 1});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);
    RecordingStateMachine rebuilt;
    node.setStateMachine(&rebuilt);

    CHK(state.role() == Role::Follower);
    CHK(state.commitIndex() == kNoIndex);
    CHK(rebuilt.indices.size() == 0);

    CHK(node.tick() == Status::OK); // re-elects itself in term 2.

    CHK(state.role() == Role::Leader);
    CHK(state.currentTerm() == 2);
    // New term's no-op at index 4 commits, which drags indices 1-3 with it.
    CHK(state.commitIndex() == 4);
    CHK(rebuilt.indices.size() == 2);
    CHK(rebuilt.payloads[0] == "PUT a 1");
    CHK(rebuilt.payloads[1] == "DEL a");
}

static void run_tests() {
    RUN(lone_node_elects_itself);
    RUN(propose_commits_and_applies);
    RUN(restart_replays_from_start);
}

REGISTER_TEST_SUITE();
