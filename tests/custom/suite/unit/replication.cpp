// Unit Replication Test Suite
// Verifies handleAppendEntries() (receiver side) and replicateTo()'s
// args-building (sender side) in isolation -- direct calls and captured
// outgoing args, not multi-node round trips. The Concurrency suite's
// log_conflict_resolution.cpp already covers truncate-on-conflict and
// leave-matching-entries-alone in depth; this file covers the
// complementary rules those scenarios don't specifically target.
//
// Covers:
// - stale-term rejection, missing-previous-entry rejection, an
//   entries-less heartbeat, and a plain append into an empty log
// - commitIndex capped at the follower's own log length, and never
//   decreasing even if a later request carries a lower leaderCommit
// - a Candidate stepping down on a valid same-term AppendEntries
// - replicateTo() computing the correct prevLogIndex/prevLogTerm and
//   entries for a peer, including sending no entries when it's already
//   caught up

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

static void rejects_stale_leader_term() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    CHK(state.observeTerm(2) == Status::OK);
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    AppendEntriesArgs args;
    args.term = 1; // Stale.
    args.leaderId = "L";

    AppendEntriesReply reply;
    CHK(node.handleAppendEntries(args, reply) == Status::OK);
    CHK(!reply.success);
    CHK(reply.term == 2);
}

static void rejects_missing_previous_entry() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = "L";
    args.prevLogIndex = 5; // Follower's log is empty -- nowhere near this.
    args.prevLogTerm = 1;

    AppendEntriesReply reply;
    CHK(node.handleAppendEntries(args, reply) == Status::OK);
    CHK(!reply.success);
    CHK(reply.matchIndex == kNoIndex);
}

static void accepts_heartbeat_with_no_entries() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = "L"; // prevLogIndex/prevLogTerm default to kNoIndex/kNoTerm; no entries.

    AppendEntriesReply reply;
    CHK(node.handleAppendEntries(args, reply) == Status::OK);
    CHK(reply.success);
    CHK(reply.matchIndex == kNoIndex);
    CHK(state.log().lastIndex() == kNoIndex);
}

static void appends_entries_to_empty_log() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = "L";
    LogEntry e1;
    e1.term = 1;
    LogEntry e2;
    e2.term = 1;
    args.entries.push_back(e1);
    args.entries.push_back(e2);

    AppendEntriesReply reply;
    CHK(node.handleAppendEntries(args, reply) == Status::OK);
    CHK(reply.success);
    CHK(reply.matchIndex == 2);
    CHK(state.log().lastIndex() == 2);
}

static void commit_capped_at_log_length() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = "L";
    LogEntry e1;
    e1.term = 1;
    args.entries.push_back(e1); // Only 1 entry -- lastIndex will be 1.
    args.leaderCommit = 10;     // Far beyond what this follower actually has.

    AppendEntriesReply reply;
    CHK(node.handleAppendEntries(args, reply) == Status::OK);
    CHK(reply.success);
    CHK(state.commitIndex() == 1); // Capped at the follower's own lastIndex, not leaderCommit.
}

static void commit_index_never_decreases() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000, 1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);

    AppendEntriesArgs args1;
    args1.term = 1;
    args1.leaderId = "L";
    for (int i = 0; i < 3; ++i) {
        LogEntry e;
        e.term = 1;
        args1.entries.push_back(e);
    }
    args1.leaderCommit = 3;

    AppendEntriesReply reply1;
    CHK(node.handleAppendEntries(args1, reply1) == Status::OK);
    CHK(state.commitIndex() == 3);

    // A later request (e.g. reordered/stale) whose entries already match
    // but carries a LOWER leaderCommit -- must not regress commitIndex.
    AppendEntriesArgs args2;
    args2.term = 1;
    args2.leaderId = "L";
    for (int i = 0; i < 3; ++i) {
        LogEntry e;
        e.term = 1;
        args2.entries.push_back(e);
    }
    args2.leaderCommit = 1;

    AppendEntriesReply reply2;
    CHK(node.handleAppendEntries(args2, reply2) == Status::OK);
    CHK(state.commitIndex() == 3); // Unchanged.
}

static void candidate_steps_down_on_heartbeat() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "A");
    SimulatedTransport transport;
    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{}, 1, 1, 50);
    CHK(state.startElection() == Status::OK); // Candidate, term 1.

    AppendEntriesArgs args;
    args.term = 1; // Same term -- a legitimate leader already won this term's election.
    args.leaderId = "L";

    AppendEntriesReply reply;
    CHK(node.handleAppendEntries(args, reply) == Status::OK);
    CHK(reply.success);
    CHK(state.role() == Role::Follower); // Stepped down.
}

static void replicate_sends_correct_prev_log() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "L");
    state.becomeLeader(Vector<NodeId>{"B"});

    LogIndex idx;
    CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK); // index 1
    CHK(storage.append(2, Vector<std::uint8_t>{}, idx) == Status::OK); // index 2
    state.leaderState().nextIndex["B"] = 2;                            // B already has index 1.

    SimulatedTransport transport;
    AppendEntriesArgs captured;
    bool received = false;
    transport.registerNode(
        "B", [](const RequestVoteArgs&, RequestVoteReply&) { return Status::OK; }, // Never called.
        [&](const AppendEntriesArgs& args, AppendEntriesReply& reply) {
            captured = args;
            received = true;
            reply.success = true;
            reply.term = args.term;
            reply.matchIndex = args.prevLogIndex + args.entries.size();
            return Status::OK;
        });

    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1,
                  1); // heartbeatIntervalTicks = 1.

    CHK(node.tick() == Status::OK); // Leader branch -- fires a heartbeat immediately.
    transport.pump();

    CHK(received);
    CHK(captured.prevLogIndex == 1);
    CHK(captured.prevLogTerm == 1);
    CHK(captured.entries.size() == 1);
    CHK(captured.entries[0].term == 2);
}

static void heartbeat_carries_no_entries() {
    InMemoryStorage storage;
    InMemoryPersistentState persist;
    NodeState state(persist, storage, "L");
    state.becomeLeader(Vector<NodeId>{"B"});

    LogIndex idx;
    CHK(storage.append(1, Vector<std::uint8_t>{}, idx) == Status::OK);
    state.leaderState().nextIndex["B"] = 2; // B already has everything (lastIndex is 1).

    SimulatedTransport transport;
    AppendEntriesArgs captured;
    bool received = false;
    transport.registerNode(
        "B", [](const RequestVoteArgs&, RequestVoteReply&) { return Status::OK; },
        [&](const AppendEntriesArgs& args, AppendEntriesReply& reply) {
            captured = args;
            received = true;
            reply.success = true;
            reply.term = args.term;
            reply.matchIndex = 1;
            return Status::OK;
        });

    ScriptedRandomSource random(Vector<int>{1000});
    RaftNode node(state, transport, random, Vector<NodeId>{"B"}, 1, 1, 1);

    CHK(node.tick() == Status::OK);
    transport.pump();

    CHK(received);
    CHK(captured.entries.size() == 0);
}

static void run_tests() {
    RUN(rejects_stale_leader_term);
    RUN(rejects_missing_previous_entry);
    RUN(accepts_heartbeat_with_no_entries);
    RUN(appends_entries_to_empty_log);
    RUN(commit_capped_at_log_length);
    RUN(commit_index_never_decreases);
    RUN(candidate_steps_down_on_heartbeat);
    RUN(replicate_sends_correct_prev_log);
    RUN(heartbeat_carries_no_entries);
}

REGISTER_TEST_SUITE();
