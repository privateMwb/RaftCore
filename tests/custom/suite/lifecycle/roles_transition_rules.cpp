// Lifecycle Role Transition Rules Test Suite
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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void become_leader_always_resets_state() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    CHK(state.startElection() == Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    state.leaderState().nextIndex["B"] = 99; // Simulate some leadership progress.
    state.leaderState().matchIndex["B"] = 50;

    // A later election win (a new term) and another becomeLeader() call
    // must not carry any of that over.
    CHK(state.startElection() == Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    CHK(state.leaderState().nextIndex["B"] == 1);         // Freshly computed, not 99.
    CHK(state.leaderState().matchIndex["B"] == kNoIndex); // Not 50.
}

static void stepping_down_clears_leader_state() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    CHK(state.startElection() == Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    state.leaderState().nextIndex["B"] = 99;

    state.stepDownToFollower();
    CHK(state.role() == Role::Follower);

    CHK(state.startElection() == Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    CHK(state.leaderState().nextIndex["B"] == 1); // Fresh, not 99.
}

static void observe_term_clears_leader_state() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    CHK(state.startElection() == Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    state.leaderState().nextIndex["B"] = 99;

    CHK(state.observeTerm(100) == Status::OK); // A much higher term arrives.
    CHK(state.role() == Role::Follower);

    CHK(state.startElection() == Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    CHK(state.leaderState().nextIndex["B"] == 1); // Fresh, not 99.
}

static void run_tests() {
    RUN(become_leader_always_resets_state);
    RUN(stepping_down_clears_leader_state);
    RUN(observe_term_clears_leader_state);
}

REGISTER_TEST_SUITE();
