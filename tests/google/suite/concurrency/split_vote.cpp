// Concurrency Split Vote Test Suite (GoogleTest)
// Verifies RaftNode's behavior when two candidates compete in the same
// term on a 4-node cluster (majority 3): neither can win on votes alone
// (self + one follower = 2 < 3), and the cluster still converges once
// one candidate's randomized election timeout fires again before the
// other's.
//
// Covers:
// - an engineered split: candidate A wins follower C's vote, candidate B
//   wins follower D's vote, and both candidates end the round stuck at 2
//   votes each, still Candidate, neither having reached majority
// - the escape: A's timeout fires again first, A wins the next term
//   outright (C and D haven't voted in the new term yet); B steps down
//   to Follower the moment A's RequestVote for the new term reaches it
//   (not specifically the heartbeat -- any RPC with a newer term does
//   this), and a second pump() confirms the no-op entry itself also
//   reaches B

#include <gtest/gtest.h>
#include <support/framework.h>

using namespace RaftCore;
using namespace RaftCore::Test;

namespace {

// Registers all four nodes' handlers with `transport`, wiring each
// straight to the matching RaftNode's own handleRequestVote()/
// handleAppendEntries() -- no reimplementation of Raft's rules here.
void registerCluster(SimulatedTransport& transport, RaftNode& a, RaftNode& b, RaftNode& c,
                     RaftNode& d) {
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
    transport.registerNode(
        "D",
        [&](const RequestVoteArgs& args, RequestVoteReply& reply) {
            return d.handleRequestVote(args, reply);
        },
        [&](const AppendEntriesArgs& args, AppendEntriesReply& reply) {
            return d.handleAppendEntries(args, reply);
        });
}

} // namespace

// Verifies the engineered split itself: A and B both time out into
// Candidate on the same tick and the same term; C's vote is steered to
// A and D's to B (by dropping A's message to D before it can be
// delivered, so D only ever sees B's request); neither reaches the
// 4-node cluster's majority of 3, and both remain Candidate afterward.
TEST(SplitVoteTest, SplitVoteReachesDeadlock) {
    SimulatedTransport transport;
    InMemoryStorage storageA, storageB, storageC, storageD;
    InMemoryPersistentState persistA, persistB, persistC, persistD;
    NodeState stateA(persistA, storageA, "A");
    NodeState stateB(persistB, storageB, "B");
    NodeState stateC(persistC, storageC, "C");
    NodeState stateD(persistD, storageD, "D");

    // A and B time out on their very first tick(); C and D are scripted
    // never to time out spontaneously during this test.
    ScriptedRandomSource randomA(Vector<int>{1, 1});
    ScriptedRandomSource randomB(Vector<int>{1, 1000});
    ScriptedRandomSource randomC(Vector<int>{1000, 1000});
    ScriptedRandomSource randomD(Vector<int>{1000, 1000});

    RaftNode nodeA(stateA, transport, randomA, Vector<NodeId>{"B", "C", "D"}, 1, 1, 50);
    RaftNode nodeB(stateB, transport, randomB, Vector<NodeId>{"A", "C", "D"}, 1, 1, 50);
    RaftNode nodeC(stateC, transport, randomC, Vector<NodeId>{"A", "B", "D"}, 1, 1, 50);
    RaftNode nodeD(stateD, transport, randomD, Vector<NodeId>{"A", "B", "C"}, 1, 1, 50);
    registerCluster(transport, nodeA, nodeB, nodeC, nodeD);

    ASSERT_EQ(nodeA.tick(), Status::OK); // A -> Candidate, term 1, queues to B/C/D.
    ASSERT_EQ(nodeB.tick(), Status::OK); // B -> Candidate, term 1, queues to A/C/D.
    EXPECT_EQ(stateA.role(), Role::Candidate);
    EXPECT_EQ(stateB.role(), Role::Candidate);
    EXPECT_EQ(stateA.currentTerm(), 1);
    EXPECT_EQ(stateB.currentTerm(), 1);

    ASSERT_TRUE(transport.dropPendingTo("D")); // A's request to D never arrives.
    ASSERT_TRUE(transport.pumpNextTo("C"));    // C sees A first -> grants A.
    ASSERT_TRUE(transport.pumpNextTo("D"));    // D now sees B first -> grants B.
    ASSERT_TRUE(transport.pumpNextTo("B"));    // B sees A's request -> already self-voted, rejects.
    ASSERT_TRUE(transport.pumpNextTo("A"));    // A sees B's request -> already self-voted, rejects.

    // 2 votes each (self + one follower) on a 4-node cluster: below the
    // majority of 3. Neither should have become Leader.
    EXPECT_EQ(stateA.role(), Role::Candidate);
    EXPECT_EQ(stateB.role(), Role::Candidate);
}

// Continues past the deadlock: A's next randomized timeout fires before
// B's (scripted), so A alone starts term 2. C and D haven't voted in
// term 2 yet, so A wins outright without B even competing this round.
// B steps down the moment A's RequestVote for term 2 reaches it.
TEST(SplitVoteTest, SplitVoteResolvesOnRetry) {
    SimulatedTransport transport;
    InMemoryStorage storageA, storageB, storageC, storageD;
    InMemoryPersistentState persistA, persistB, persistC, persistD;
    NodeState stateA(persistA, storageA, "A");
    NodeState stateB(persistB, storageB, "B");
    NodeState stateC(persistC, storageC, "C");
    NodeState stateD(persistD, storageD, "D");

    ScriptedRandomSource randomA(Vector<int>{1, 1});
    ScriptedRandomSource randomB(Vector<int>{1, 1000}); // Won't retry during round 2.
    ScriptedRandomSource randomC(Vector<int>{1000, 1000});
    ScriptedRandomSource randomD(Vector<int>{1000, 1000});

    RaftNode nodeA(stateA, transport, randomA, Vector<NodeId>{"B", "C", "D"}, 1, 1, 50);
    RaftNode nodeB(stateB, transport, randomB, Vector<NodeId>{"A", "C", "D"}, 1, 1, 50);
    RaftNode nodeC(stateC, transport, randomC, Vector<NodeId>{"A", "B", "D"}, 1, 1, 50);
    RaftNode nodeD(stateD, transport, randomD, Vector<NodeId>{"A", "B", "C"}, 1, 1, 50);
    registerCluster(transport, nodeA, nodeB, nodeC, nodeD);

    // Round 1: reproduce the same deadlock as SplitVoteReachesDeadlock.
    ASSERT_EQ(nodeA.tick(), Status::OK);
    ASSERT_EQ(nodeB.tick(), Status::OK);
    ASSERT_TRUE(transport.dropPendingTo("D"));
    ASSERT_TRUE(transport.pumpNextTo("C"));
    ASSERT_TRUE(transport.pumpNextTo("D"));
    ASSERT_TRUE(transport.pumpNextTo("B"));
    ASSERT_TRUE(transport.pumpNextTo("A"));
    ASSERT_EQ(stateA.role(), Role::Candidate);
    ASSERT_EQ(stateB.role(), Role::Candidate);

    // Round 2: only A retries (B's script keeps it quiet this round).
    ASSERT_EQ(nodeA.tick(), Status::OK); // A -> Candidate, term 2, queues to B/C/D.
    ASSERT_EQ(stateA.currentTerm(), 2);

    // A single pump() delivers all three RequestVotes: C and D haven't
    // voted in term 2 yet and grant A outright -- A reaches majority
    // partway through this same pump() call, becomes Leader, and
    // immediately queues its no-op-entry AppendEntries to everyone. B,
    // on the very first of these RequestVotes to reach it, adopts term 2
    // via the "All Servers" rule and steps down to Follower right there.
    transport.pump();
    EXPECT_EQ(stateA.role(), Role::Leader);
    EXPECT_EQ(stateB.role(), Role::Follower);
    EXPECT_EQ(stateB.currentTerm(), 2);

    // The no-op entry was queued to every peer within that same pump()
    // call above, but RequestVote/AppendEntries are separate FIFOs -- a
    // second pump() actually delivers it, confirming replication (not
    // just the election) reached the former rival.
    transport.pump();
    EXPECT_EQ(stateB.log().lastIndex(), 1);
}
