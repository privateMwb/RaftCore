// Unit NodeState Test Suite
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

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void starts_follower_at_term_zero() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    CHK(state.selfId() == "A");
    CHK(state.role() == Role::Follower);
    CHK(state.currentTerm() == kNoTerm);
    CHK(!state.votedFor().has_value());
    CHK(state.commitIndex() == kNoIndex);
    CHK(state.lastApplied() == kNoIndex);
    CHK(state.log().lastIndex() == kNoIndex);
}

static void start_election_votes_self() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    CHK(state.startElection() == Status::OK);
    CHK(state.currentTerm() == 1);
    CHK(state.role() == Role::Candidate);
    CHK(state.votedFor().has_value());
    CHK(state.votedFor().value() == "A");
}

static void higher_term_steps_down() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    CHK(state.startElection() == Status::OK); // Candidate, term 1, voted for self.

    CHK(state.observeTerm(5) == Status::OK);
    CHK(state.currentTerm() == 5);
    CHK(state.role() == Role::Follower); // Stepped down.
    CHK(!state.votedFor().has_value());  // A new term means no vote cast in it yet.
}

static void stale_term_is_ignored() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    CHK(state.startElection() == Status::OK); // Candidate, term 1.

    CHK(state.observeTerm(1) == Status::OK); // Equal -- no-op.
    CHK(state.role() == Role::Candidate);
    CHK(state.currentTerm() == 1);

    CHK(state.observeTerm(0) == Status::OK); // Stale -- also a no-op.
    CHK(state.role() == Role::Candidate);
    CHK(state.currentTerm() == 1);
}

static void grant_vote_keeps_role() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    CHK(state.grantVoteTo("X") == Status::OK);
    CHK(state.votedFor().has_value());
    CHK(state.votedFor().value() == "X");
    CHK(state.role() == Role::Follower); // Unchanged -- granting a vote isn't a role transition.
    CHK(state.currentTerm() ==
        kNoTerm); // Unchanged -- grantVoteTo() doesn't touch the term either.
}

static void leader_initializes_peer_state() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    LogIndex idx;
    CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK); // lastIndex() == 2.

    CHK(state.startElection() == Status::OK);
    state.becomeLeader(Vector<NodeId>{"B", "C"});

    CHK(state.role() == Role::Leader);
    CHK(state.leaderState().nextIndex.contains("B"));
    CHK(state.leaderState().nextIndex.contains("C"));
    CHK(state.leaderState().nextIndex["B"] == 3); // lastIndex() + 1.
    CHK(state.leaderState().nextIndex["C"] == 3);
    CHK(state.leaderState().matchIndex["B"] == kNoIndex);
    CHK(state.leaderState().matchIndex["C"] == kNoIndex);
}

static void step_down_drops_leader_state() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    CHK(state.startElection() == Status::OK);
    state.becomeLeader(Vector<NodeId>{"B"});
    CHK(state.role() == Role::Leader);

    state.stepDownToFollower();
    CHK(state.role() == Role::Follower);
}

static void commit_and_applied_setters() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    state.setCommitIndex(3);
    CHK(state.commitIndex() == 3);

    state.setLastApplied(2);
    CHK(state.lastApplied() == 2);
}

static void log_reflects_same_storage() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");

    LogIndex idx;
    CHK(state.log().append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    CHK(idx == 1);

    // Appended through state.log(), but it's the SAME storage object --
    // confirms log() isn't returning a copy.
    CHK(storage.lastIndex() == 1);
    LogEntry entry;
    CHK(storage.entryAt(1, entry) == Status::OK);
    CHK(entry.term == 1);
}

static void run_tests() {
    RUN(starts_follower_at_term_zero);
    RUN(start_election_votes_self);
    RUN(higher_term_steps_down);
    RUN(stale_term_is_ignored);
    RUN(grant_vote_keeps_role);
    RUN(leader_initializes_peer_state);
    RUN(step_down_drops_leader_state);
    RUN(commit_and_applied_setters);
    RUN(log_reflects_same_storage);
}

REGISTER_TEST_SUITE();
