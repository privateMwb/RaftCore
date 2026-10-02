// Concurrency Log Conflict Resolution Test Suite
// Verifies handleAppendEntries()'s reconciliation logic directly:
// conflicting entries get truncated and overwritten with the leader's
// authoritative version, and entries that already match are left alone
// -- unlike the other Concurrency files, this doesn't need tick() or a
// transport at all, since the behavior under test is entirely
// contained in a single RPC handler call.
//
// Covers:
// - a follower with a stale, conflicting entry at some index correctly
//   truncates it and adopts the leader's entry once an authoritative
//   AppendEntries covering that index arrives
// - a retried/duplicate AppendEntries carrying entries the follower
//   already has, exactly matching, is a no-op -- critically, it must
//   not truncate and drop a later entry the request doesn't even
//   mention

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

// A follower's log has a stale entry (term 1) at index 2, left over from
// a term that never got the entry committed. A real leader's
// AppendEntries, carrying its own authoritative entry (term 2) at that
// same index, arrives -- the follower must truncate the stale entry and
// adopt the leader's.
static void conflicting_entry_gets_overwritten() {
    SimulatedTransport transport; // Unused here -- only needed for RaftNode's constructor.
    InMemoryStorage storageF;
    InMemoryPersistentState persistF;
    NodeState stateF(persistF, storageF, "F");
    ScriptedRandomSource randomF(Vector<int>{1000});
    RaftNode nodeF(stateF, transport, randomF, Vector<NodeId>{}, 1, 1, 50);

    LogIndex idx;
    CHK(storageF.append(1, Vector<std::uint8_t>{}, idx) ==
        Status::OK); // index 1, term 1 -- matches the leader.
    CHK(storageF.append(1, Vector<std::uint8_t>{}, idx) ==
        Status::OK); // index 2, term 1 -- the leader's is term 2.
    CHK(persistF.setCurrentTerm(1) == Status::OK);

    AppendEntriesArgs args;
    args.term = 2;
    args.leaderId = "L";
    args.prevLogIndex = 1;
    args.prevLogTerm = 1; // Matches F's index 1, so the consistency check passes.
    LogEntry authoritative;
    authoritative.term = 2;
    args.entries.push_back(authoritative);
    args.leaderCommit = 0;

    AppendEntriesReply reply;
    CHK(nodeF.handleAppendEntries(args, reply) == Status::OK);
    CHK(reply.success);
    CHK(reply.term == 2);
    CHK(stateF.currentTerm() == 2); // Adopted the leader's term via the "All Servers" rule.
    CHK(stateF.log().lastIndex() == 2);

    LogEntry checkEntry;
    CHK(storageF.entryAt(2, checkEntry) == Status::OK);
    CHK(checkEntry.term == 2); // The stale term-1 entry was truncated and replaced.
}

// A retried or duplicate AppendEntries carrying entries the follower
// already has, exactly matching, must be a pure no-op for those indices
// -- and critically must not truncate and drop a later entry the
// request doesn't even cover.
static void matching_entries_left_alone() {
    SimulatedTransport transport;
    InMemoryStorage storageF;
    InMemoryPersistentState persistF;
    NodeState stateF(persistF, storageF, "F");
    ScriptedRandomSource randomF(Vector<int>{1000});
    RaftNode nodeF(stateF, transport, randomF, Vector<NodeId>{}, 1, 1, 50);

    LogIndex idx;
    CHK(storageF.append(1, Vector<std::uint8_t>{}, idx) == Status::OK); // index 1
    CHK(storageF.append(1, Vector<std::uint8_t>{}, idx) == Status::OK); // index 2
    CHK(storageF.append(2, Vector<std::uint8_t>{}, idx) ==
        Status::OK); // index 3 -- not covered below.
    CHK(persistF.setCurrentTerm(2) == Status::OK);

    AppendEntriesArgs args;
    args.term = 2;
    args.leaderId = "L";
    args.prevLogIndex = 0;
    args.prevLogTerm = 0;
    LogEntry entry1;
    entry1.term = 1;
    LogEntry entry2;
    entry2.term = 1;
    args.entries.push_back(entry1);
    args.entries.push_back(entry2);
    args.leaderCommit = 0;

    AppendEntriesReply reply;
    CHK(nodeF.handleAppendEntries(args, reply) == Status::OK);
    CHK(reply.success);

    // A buggy implementation that truncates unconditionally, even when
    // entries already match, would have wiped out index 3 here.
    CHK(stateF.log().lastIndex() == 3);
    LogEntry checkEntry;
    CHK(storageF.entryAt(3, checkEntry) == Status::OK);
    CHK(checkEntry.term == 2);
}

static void run_tests() {
    RUN(conflicting_entry_gets_overwritten);
    RUN(matching_entries_left_alone);
}

REGISTER_TEST_SUITE();
