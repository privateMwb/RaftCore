// Lifecycle Status Propagation Test Suite
// Verifies that a failing Storage/PersistentState call is never silently
// swallowed anywhere in the call chain -- using FailingStorage/
// FailingPersistentState (reference.h), which fail universally rather
// than modeling any realistic intermittent failure, just to force each
// path reliably.
//
// Covers:
// - a PersistentState failure during election blocks the transition
//   entirely (no partial term bump, no partial vote)
// - a Storage failure during AppendEntries reconciliation propagates
//   instead of reporting success
// - the one case where failure doesn't block a transition: becomeLeader()
//   itself can't fail (it's void), so a Storage failure on the no-op
//   entry that follows leaves the role already changed to Leader -- the
//   failure is still surfaced, just via takeLastAsyncError() rather than
//   by preventing the transition

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void persistent_state_blocks_election() {
    InMemoryStorage storage;
    FailingPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1, 50);

    CHK(node.tick() == Status::IO_ERROR); // The election timeout fires, but the term bump fails.
    CHK(state.role() == Role::Follower);  // No partial transition.
    CHK(state.currentTerm() == kNoTerm);
}

static void append_entries_failure_propagates() {
    FailingStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = "L";
    LogEntry entry;
    entry.term = 1;
    args.entries.push_back(entry);

    AppendEntriesReply reply;
    CHK(node.handleAppendEntries(args, reply) == Status::IO_ERROR);
}

static void leader_transition_survives_storage_failure() {
    FailingStorage storage;
    InMemoryPersistentState persist; // Term/vote bookkeeping succeeds; only the log fails.
    NodeState state(persist, storage, "A");
    SimulatedTransport transport; // Unused -- the reply is driven by hand below.
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B", "C"}, 1, 1, 50);

    CHK(state.startElection() == Status::OK); // Candidate, term 1 -- doesn't touch Storage.

    RequestVoteReply replyB;
    replyB.term = 1;
    replyB.voteGranted = true;
    node.handleRequestVoteReply("B", replyB); // self + B = majority of this 3-node cluster.

    // becomeLeader() itself is void and cannot fail -- the role has
    // already changed by the time the no-op entry's append is attempted
    // and fails.
    CHK(state.role() == Role::Leader);

    std::optional<Status> error = node.takeLastAsyncError();
    CHK(error.has_value());
    CHK(error.value() == Status::IO_ERROR);
}

static void run_tests() {
    RUN(persistent_state_blocks_election);
    RUN(append_entries_failure_propagates);
    RUN(leader_transition_survives_storage_failure);
}

REGISTER_TEST_SUITE();
