// Lifecycle Term Monotonicity Test Suite
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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void term_never_decreases_on_stale() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    CHK(state.observeTerm(5) == Status::OK);
    CHK(state.currentTerm() == 5);

    CHK(state.observeTerm(2) == Status::OK); // Stale -- ignored.
    CHK(state.currentTerm() == 5);

    CHK(state.observeTerm(5) == Status::OK); // Equal -- also ignored.
    CHK(state.currentTerm() == 5);
}

static void term_only_rises_through_elections() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    Term previous = state.currentTerm();
    for (int i = 0; i < 3; ++i) {
        CHK(state.startElection() == Status::OK);
        CHK(state.currentTerm() > previous); // Strictly greater each time.
        previous = state.currentTerm();
    }
    CHK(state.currentTerm() == 3);
}

static void commit_index_never_regresses_leader() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "L");

    CHK(state.startElection() ==
        Status::OK); // Term 1, so entries below can legitimately be committed.
    state.becomeLeader(Vector<NodeId>{"B"});

    LogIndex idx;
    for (int i = 0; i < 3; ++i) {
        CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    }

    SimulatedTransport transport; // Unused -- replies are driven by hand below.
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1, 50);

    AppendEntriesReply highReply;
    highReply.term = state.currentTerm();
    highReply.success = true;
    highReply.matchIndex = 3;
    node.handleAppendEntriesReply("B", highReply);
    CHK(state.commitIndex() == 3); // self + B reaches this 2-node cluster's majority.

    // A delayed or reordered reply arrives afterward, reporting a LOWER
    // matchIndex than one already seen for the same peer.
    AppendEntriesReply lowReply;
    lowReply.term = state.currentTerm();
    lowReply.success = true;
    lowReply.matchIndex = 1;
    node.handleAppendEntriesReply("B", lowReply);
    CHK(state.commitIndex() == 3); // Unchanged -- never regresses.
}

static void run_tests() {
    RUN(term_never_decreases_on_stale);
    RUN(term_only_rises_through_elections);
    RUN(commit_index_never_regresses_leader);
}

REGISTER_TEST_SUITE();
