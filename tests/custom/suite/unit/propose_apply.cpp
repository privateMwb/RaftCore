// Unit Propose / Apply Test Suite
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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void propose_on_follower_rejected() {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B", "C"}, 1, 1, 1);

    LogIndex index = 99;
    CHK(node.propose(makePayload("PUT a 1"), index) == Status::NOT_LEADER);
    CHK(index == 99); // untouched on failure.
    CHK(storage.lastIndex() == kNoIndex);
}

static void propose_rejects_empty_payload() {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B", "C"}, 1, 1, 1);

    state.becomeLeader(Vector<NodeId>{"B", "C"});

    LogIndex index = 99;
    CHK(node.propose(Vector<std::uint8_t>{}, index) == Status::INVALID_ARGUMENT);
    CHK(index == 99);
    CHK(storage.lastIndex() == kNoIndex);
}

static void apply_without_state_machine() {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);

    LogIndex index;
    CHK(storage.append(1, makePayload("x"), index) == Status::OK);
    state.setCommitIndex(1);

    CHK(node.applyCommitted() == Status::OK);
    CHK(state.lastApplied() == kNoIndex); // nothing consumed it.
}

static void apply_in_order_skips_noop() {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);
    RecordingStateMachine machine;
    node.setStateMachine(&machine);

    LogIndex index;
    CHK(storage.append(1, Vector<std::uint8_t>{}, index) == Status::OK); // election no-op.
    CHK(storage.append(1, makePayload("first"), index) == Status::OK);
    CHK(storage.append(1, makePayload("second"), index) == Status::OK);
    CHK(storage.append(1, makePayload("third"), index) == Status::OK);

    state.setCommitIndex(3); // first two committed; "third" is not.
    CHK(node.applyCommitted() == Status::OK);

    CHK(machine.indices.size() == 2);
    CHK(machine.indices[0] == 2);
    CHK(machine.indices[1] == 3);
    CHK(machine.payloads[0] == "first");
    CHK(machine.payloads[1] == "second");
    CHK(state.lastApplied() == 3); // the no-op's index was consumed too.

    // Idempotent until commitIndex moves again.
    CHK(node.applyCommitted() == Status::OK);
    CHK(machine.indices.size() == 2);

    state.setCommitIndex(4);
    CHK(node.applyCommitted() == Status::OK);
    CHK(machine.indices.size() == 3);
    CHK(machine.indices[2] == 4);
    CHK(machine.payloads[2] == "third");
}

static void late_bound_machine_catches_up() {
    SimulatedTransport transport;
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 1);

    LogIndex index;
    CHK(storage.append(1, makePayload("a"), index) == Status::OK);
    CHK(storage.append(1, makePayload("b"), index) == Status::OK);
    state.setCommitIndex(2);
    CHK(node.applyCommitted() == Status::OK); // unbound: nothing happens.

    RecordingStateMachine machine;
    node.setStateMachine(&machine);
    CHK(node.applyCommitted() == Status::OK);

    CHK(machine.indices.size() == 2);
    CHK(machine.payloads[0] == "a");
    CHK(machine.payloads[1] == "b");
}

static void run_tests() {
    RUN(propose_on_follower_rejected);
    RUN(propose_rejects_empty_payload);
    RUN(apply_without_state_machine);
    RUN(apply_in_order_skips_noop);
    RUN(late_bound_machine_catches_up);
}

REGISTER_TEST_SUITE();
