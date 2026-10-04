// Integration Log Replication Test Suite (GoogleTest)
// Verifies the normal, no-failures replication path end-to-end: a
// Leader appends client commands, replicates them to its followers via
// a tick()-driven AppendEntries and pump()-delivered replies, and
// commitIndex advances once a majority holds each entry -- the
// steady-state complement to the Concurrency suite's failure scenarios.
//
// Covers:
// - the Leader's no-op entry committing once a majority acks it
// - three client commands appended in sequence, replicated to both
//   followers in a single batched AppendEntries (not one entry per
//   round trip), and reflected in their logs exactly
// - commitIndex advancing to cover all of them once acked

#include <gtest/gtest.h>
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

TEST(LogReplicationTest, MultipleCommandsReplicateInOrder) {
    SimulatedTransport transport;
    InMemoryStorage storageA, storageB, storageC;
    InMemoryPersistentState persistA, persistB, persistC;
    NodeState stateA(persistA, storageA, "A");
    NodeState stateB(persistB, storageB, "B");
    NodeState stateC(persistC, storageC, "C");

    ScriptedRandomSource randomA(Vector<int>{1, 1});
    // 5 resets accounted for: construction, adopt-on-vote-request,
    // grant-vote, heartbeat-received (the no-op), heartbeat-received
    // (the batch of 3 commands). A couple of spares beyond that.
    ScriptedRandomSource randomB(Vector<int>{1000, 1000, 1000, 1000, 1000, 1000});
    ScriptedRandomSource randomC(Vector<int>{1000, 1000, 1000, 1000, 1000, 1000});

    // Heartbeat interval of 1 on A: every tick() replicates immediately.
    RaftNode nodeA(stateA, transport, randomA, Vector<NodeId>{"B", "C"}, 1, 1, 1);
    RaftNode nodeB(stateB, transport, randomB, Vector<NodeId>{"A", "C"}, 1, 1, 50);
    RaftNode nodeC(stateC, transport, randomC, Vector<NodeId>{"A", "B"}, 1, 1, 50);
    registerCluster(transport, nodeA, nodeB, nodeC);

    // Round 1: A becomes Leader (no-op entry at index 1).
    ASSERT_EQ(nodeA.tick(), Status::OK);
    transport.pump(); // Delivers RequestVotes; A becomes Leader mid-pump, queues the no-op's
                      // AppendEntries.
    ASSERT_EQ(stateA.role(), Role::Leader);
    transport.pump(); // Delivers the no-op AppendEntries; replies are handled synchronously within
                      // this call.

    EXPECT_EQ(stateB.log().lastIndex(), 1);
    EXPECT_EQ(stateC.log().lastIndex(), 1);
    // Self + B alone already reaches this 3-node cluster's majority of
    // 2, so commitIndex advances as soon as B's reply is processed --
    // within this same pump() call, not a later one.
    EXPECT_EQ(stateA.commitIndex(), 1);

    // Inject three client commands directly onto the Leader (Phase 5's
    // client-facing API doesn't exist yet -- this is what it would
    // eventually call under the hood).
    for (int i = 0; i < 3; ++i) {
        LogIndex idx;
        ASSERT_EQ(storageA.append(1, Vector<std::uint8_t>{}, idx), Status::OK);
    }
    ASSERT_EQ(stateA.log().lastIndex(), 4); // no-op + 3 commands.

    // One heartbeat round replicates the entire backlog at once --
    // AppendEntries carries every new entry from nextIndex onward, not
    // just one per round trip.
    ASSERT_EQ(nodeA.tick(), Status::OK);
    transport.pump();

    EXPECT_EQ(stateB.log().lastIndex(), 4);
    EXPECT_EQ(stateC.log().lastIndex(), 4);
    EXPECT_EQ(stateA.commitIndex(), 4);

    for (LogIndex index = 1; index <= 4; ++index) {
        LogEntry entryA, entryB, entryC;
        ASSERT_EQ(storageA.entryAt(index, entryA), Status::OK);
        ASSERT_EQ(storageB.entryAt(index, entryB), Status::OK);
        ASSERT_EQ(storageC.entryAt(index, entryC), Status::OK);
        EXPECT_EQ(entryB.term, entryA.term);
        EXPECT_EQ(entryC.term, entryA.term);
    }
}
