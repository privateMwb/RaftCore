// Integration Single Node Election Test Suite (GoogleTest)
// Verifies a full election round trip end-to-end over SimulatedTransport,
// exactly as a real deployment's event loop would drive it: a node's
// election timeout fires, RequestVote RPCs go out over the transport,
// votes come back, majority is reached, and the resulting Leader
// appends and replicates its no-op entry -- nothing here calls
// RaftNode's RPC handlers directly, only tick() and pump().
//
// Covers:
// - a 3-node cluster's sole timed-out node winning an uncontested
//   election: role transitions, term, and the self-vote are all correct
// - the no-op entry (the Figure 8 fix) is appended on election and
//   reaches both followers before the test ends

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

TEST(SingleNodeElectionTest, UncontestedElectionElectsLeader) {
    SimulatedTransport transport;
    InMemoryStorage storageA, storageB, storageC;
    InMemoryPersistentState persistA, persistB, persistC;
    NodeState stateA(persistA, storageA, "A");
    NodeState stateB(persistB, storageB, "B");
    NodeState stateC(persistC, storageC, "C");

    // A times out immediately; B and C are scripted never to compete.
    ScriptedRandomSource randomA(Vector<int>{1, 1});
    ScriptedRandomSource randomB(Vector<int>{1000, 1000, 1000, 1000});
    ScriptedRandomSource randomC(Vector<int>{1000, 1000, 1000, 1000});

    RaftNode nodeA(stateA, transport, randomA, Vector<NodeId>{"B", "C"}, 1, 1, 50);
    RaftNode nodeB(stateB, transport, randomB, Vector<NodeId>{"A", "C"}, 1, 1, 50);
    RaftNode nodeC(stateC, transport, randomC, Vector<NodeId>{"A", "B"}, 1, 1, 50);
    registerCluster(transport, nodeA, nodeB, nodeC);

    EXPECT_EQ(stateA.role(), Role::Follower); // Starting point, before anything happens.

    ASSERT_EQ(nodeA.tick(), Status::OK); // A's timeout fires -> Candidate, term 1, votes self.
    EXPECT_EQ(stateA.role(), Role::Candidate);
    EXPECT_EQ(stateA.currentTerm(), 1);
    ASSERT_TRUE(stateA.votedFor().has_value());
    EXPECT_EQ(stateA.votedFor().value(), "A");

    transport.pump(); // Delivers A's RequestVotes; both grant (neither has voted this term).
    ASSERT_EQ(stateA.role(), Role::Leader);
    ASSERT_TRUE(stateB.votedFor().has_value());
    ASSERT_TRUE(stateC.votedFor().has_value());
    EXPECT_EQ(stateB.votedFor().value(), "A");
    EXPECT_EQ(stateC.votedFor().value(), "A");

    // Leadership alone doesn't replicate anything yet -- the no-op entry
    // was appended locally the instant A became Leader, but reaching
    // B/C is a separate send, queued at that same moment and delivered
    // by the next pump().
    EXPECT_EQ(stateA.log().lastIndex(), 1);
    EXPECT_EQ(stateB.log().lastIndex(), 0);
    EXPECT_EQ(stateC.log().lastIndex(), 0);

    transport.pump(); // Delivers the no-op entry itself.
    EXPECT_EQ(stateB.log().lastIndex(), 1);
    EXPECT_EQ(stateC.log().lastIndex(), 1);

    LogEntry entryB, entryC;
    ASSERT_EQ(storageB.entryAt(1, entryB), Status::OK);
    ASSERT_EQ(storageC.entryAt(1, entryC), Status::OK);
    EXPECT_EQ(entryB.term, 1);
    EXPECT_EQ(entryC.term, 1);
}
