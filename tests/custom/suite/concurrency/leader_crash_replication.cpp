// Concurrency Leader Crash Mid-Replication Test Suite
// Verifies RaftNode's behavior when a Leader crashes after replicating a
// command entry to only one of its two followers: the follower that's
// behind can't win an election on its own (the up-to-date-log election
// restriction blocks it), and once a legitimate new Leader is elected,
// the standard nextIndex back-off mechanism reconciles the lagging
// follower's log automatically.
//
// Covers:
// - A becomes Leader of a 3-node cluster, then replicates a command
//   entry to B but not C before "crashing" (A is unregistered for good)
// - C, missing that entry, attempts an election and is correctly
//   rejected by B, whose log is more up-to-date -- demonstrating the
//   election restriction's actual purpose, not just the granting rule
//   in isolation
// - B, the node with the complete log, wins the next election instead
//   (with C's vote), and its replication to C detects the conflict,
//   backs off, and retries until C's log matches exactly

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

namespace {

void registerCluster(SimulatedTransport& transport, RaftNode& a, RaftNode& b, RaftNode& c) {
    transport.registerNode(
        "A",
        [&](const RequestVoteArgs& args, RequestVoteReply& reply) {
            return a.handleRequestVote(args, reply);
        },
        [&](const AppendEntriesArgs& args, AppendEntriesReply& reply) {
            return a.handleAppendEntries(args, reply);
        });
    transport.registerNode(
        "B",
        [&](const RequestVoteArgs& args, RequestVoteReply& reply) {
            return b.handleRequestVote(args, reply);
        },
        [&](const AppendEntriesArgs& args, AppendEntriesReply& reply) {
            return b.handleAppendEntries(args, reply);
        });
    transport.registerNode(
        "C",
        [&](const RequestVoteArgs& args, RequestVoteReply& reply) {
            return c.handleRequestVote(args, reply);
        },
        [&](const AppendEntriesArgs& args, AppendEntriesReply& reply) {
            return c.handleAppendEntries(args, reply);
        });
}

} // namespace

static void lagging_follower_wins_via_backoff() {
    SimulatedTransport transport;
    InMemoryStorage storageA, storageB, storageC;
    InMemoryPersistentState persistA, persistB, persistC;
    NodeState stateA(persistA, storageA, "A");
    NodeState stateB(persistB, storageB, "B");
    NodeState stateC(persistC, storageC, "C");

    // B receives one extra reset beyond C (the dropped-to-C replication
    // RPC that DOES reach B), so B's and C's scripts diverge in length --
    // see the comments at each point they're consumed, below.
    ScriptedRandomSource randomA(Vector<int>{1, 1});
    ScriptedRandomSource randomB(Vector<int>{1000, 1000, 1000, 1000, 1000, 2});
    ScriptedRandomSource randomC(Vector<int>{1000, 1000, 1000, 3});

    RaftNode nodeA(stateA, transport, randomA, Vector<NodeId>{"B", "C"}, 1, 1, 1);
    RaftNode nodeB(stateB, transport, randomB, Vector<NodeId>{"A", "C"}, 1, 1, 50);
    RaftNode nodeC(stateC, transport, randomC, Vector<NodeId>{"A", "B"}, 1, 1, 50);
    registerCluster(transport, nodeA, nodeB, nodeC);

    // Round 1: A becomes Leader of the whole cluster (consumes each
    // script's first 4 entries: construction, adopt-on-vote-request,
    // grant-vote, heartbeat-received).
    CHK(nodeA.tick() == Status::OK);
    transport.pump();
    CHK(stateA.role() == Role::Leader);
    transport.pump(); // No-op entry (index 1) reaches B and C.
    CHK(stateB.log().lastIndex() == 1);
    CHK(stateC.log().lastIndex() == 1);

    // Simulate a client command landing on the Leader directly (Phase 5's
    // client-facing API doesn't exist yet -- this is what it would
    // eventually call under the hood).
    LogIndex commandIndex;
    CHK(storageA.append(1, Vector<std::uint8_t>{}, commandIndex) == Status::OK);
    CHK(commandIndex == 2);

    // Replicate it -- but only to B. C's copy is dropped, simulating A
    // crashing before C caught up. (B's 5th script entry is consumed
    // here, on the heartbeat it receives; C's script stays at 4 entries
    // since it never gets this one.)
    CHK(nodeA.tick() == Status::OK);
    CHK(transport.dropPendingTo("C"));
    transport.pump();
    CHK(stateB.log().lastIndex() == 2);
    CHK(stateC.log().lastIndex() == 1); // Still behind.
    // Self + B already reaches this 3-node cluster's majority of 2, so
    // the entry is committed even though C hasn't caught up yet --
    // that's the "mid-replication" part: C simply hasn't heard about it
    // when the crash happens next.
    CHK(stateA.commitIndex() == 2);

    // Crash: A is gone for good (unlike a partition, never re-registered).
    transport.unregisterNode("A");

    // C's silence budget (3 ticks, scripted) runs out first.
    for (int i = 0; i < 3; ++i) {
        CHK(nodeB.tick() == Status::OK);
        CHK(nodeC.tick() == Status::OK);
    }
    CHK(stateC.role() == Role::Candidate);
    CHK(stateC.currentTerm() == 2);

    // C's RequestVote reaches B (and fails immediately against
    // A, unregistered). B's log is more complete (index 2 vs C's index
    // 1, same term) -- the election restriction correctly rejects C.
    transport.pump();
    CHK(stateB.currentTerm() == 2);        // B still adopts the newer term...
    CHK(!stateB.votedFor().has_value());   // ...but didn't vote for C: term adoption clears
                                           // the vote, and the rejected request never re-set it.
    CHK(stateC.role() == Role::Candidate); // C's lone self-vote isn't a majority.

    // B's own silence budget (2 ticks, scripted) runs out next -- B
    // starts its own election, for term 3.
    CHK(nodeB.tick() == Status::OK);
    CHK(nodeB.tick() == Status::OK);
    CHK(stateB.currentTerm() == 3);

    // C grants B (B's log is at least as complete as C's own); B reaches
    // majority (self + C) and becomes Leader, appending its own no-op
    // entry at index 3.
    transport.pump();
    CHK(stateB.role() == Role::Leader);

    // B's replication to C conflicts (C has no entry at index 2 at all)
    // -- backs nextIndex off by one and retries immediately.
    transport.pump(); // Delivers the first (rejected) AppendEntries attempt.
    transport.pump(); // Delivers the retry, which C accepts.

    CHK(stateC.log().lastIndex() == 3);
    LogEntry recoveredEntry;
    CHK(storageC.entryAt(2, recoveredEntry) == Status::OK);
    CHK(recoveredEntry.term == 1); // C caught up on exactly what it missed.
    CHK(stateB.commitIndex() == 3);
}

static void run_tests() {
    RUN(lagging_follower_wins_via_backoff);
}

REGISTER_TEST_SUITE();
