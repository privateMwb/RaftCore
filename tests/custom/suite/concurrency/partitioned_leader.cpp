// Concurrency Partitioned Leader Test Suite
// Verifies RaftNode's behavior when a Leader is cut off from the
// majority of a 3-node cluster: the majority elects a new Leader for a
// higher term, the old Leader keeps believing it's still in charge
// (split brain) until the partition heals, and it steps down the moment
// a higher-term RPC from the new Leader reaches it.
//
// Covers:
// - A is elected Leader normally, then gets partitioned away from B/C
// - B/C, no longer hearing A's heartbeats, elect B as Leader for a new
//   term -- while A still believes (incorrectly) that it's Leader
// - healing the partition and A stepping down once B's heartbeat, from
//   a higher term, actually reaches it

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

static void partitioned_leader_steps_down_on_heal() {
    SimulatedTransport transport;
    InMemoryStorage storageA, storageB, storageC;
    InMemoryPersistentState persistA, persistB, persistC;
    NodeState stateA(persistA, storageA, "A");
    NodeState stateB(persistB, storageB, "B");
    NodeState stateC(persistC, storageC, "C");

    // A times out immediately and wins the first election outright (B/C
    // start with long timeouts so they don't compete). Every reset A/B/C
    // go through during that first election and the heartbeat that
    // follows is accounted for below, so each script's later entries
    // land exactly where this test needs them.
    ScriptedRandomSource randomA(Vector<int>{1, 1});
    // B: [construction, adopt-term-on-vote-request, grant-vote,
    //     heartbeat-received] = 4 resets during round 1, then [4] is the
    // one that actually governs how long B tolerates silence once A's
    // heartbeats stop arriving.
    ScriptedRandomSource randomB(Vector<int>{1000, 1000, 1000, 3});
    ScriptedRandomSource randomC(Vector<int>{1000, 1000, 1000, 1000});

    // A's heartbeat interval is 1 so every tick(A) call produces a fresh
    // heartbeat to drop -- makes the "still trying, but partitioned"
    // behavior explicit rather than just skipping A's ticks entirely.
    RaftNode nodeA(stateA, transport, randomA, Vector<NodeId>{"B", "C"}, 1, 1, 1);
    RaftNode nodeB(stateB, transport, randomB, Vector<NodeId>{"A", "C"}, 1, 1, 2);
    RaftNode nodeC(stateC, transport, randomC, Vector<NodeId>{"A", "B"}, 1, 1, 50);
    registerCluster(transport, nodeA, nodeB, nodeC);

    // Round 1: A becomes Leader of the whole 3-node cluster.
    CHK(nodeA.tick() == Status::OK);
    transport.pump(); // Delivers A's RequestVotes; A reaches majority mid-pump, becomes Leader.
    CHK(stateA.role() == Role::Leader);
    transport.pump(); // Delivers the no-op-entry heartbeat to B and C.
    CHK(stateB.log().lastIndex() == 1);
    CHK(stateC.log().lastIndex() == 1);

    // Partition: B/C can no longer reach A (unregistering removes both
    // its RequestVote and AppendEntries handlers). A's own sends to B/C
    // still queue successfully -- dropped explicitly below, modeling
    // packets lost in the partition rather than simply not sending them.
    transport.unregisterNode("A");

    for (int i = 0; i < 3; ++i) {
        CHK(nodeA.tick() == Status::OK); // A still believes it's Leader, tries to send heartbeats.
        CHK(transport.dropPendingTo("B"));
        CHK(transport.dropPendingTo("C"));
        CHK(nodeB.tick() == Status::OK); // No heartbeat arrives to reset B's timer anymore.
    }
    // B's silence budget (3, scripted) is exhausted: it starts an
    // election for term 2.
    CHK(stateB.currentTerm() == 2);
    CHK(stateB.role() == Role::Candidate);

    transport
        .pump(); // B's RequestVote to A fails immediately (unregistered); to C, it's delivered.
    CHK(stateC.votedFor().value() == "B");
    CHK(stateB.role() == Role::Leader); // Reached majority with just C's vote (self + C = 2 of 3).
    CHK(stateB.currentTerm() == 2);

    // Split brain: A still thinks it's Leader of term 1, with no idea
    // anything happened.
    CHK(stateA.role() == Role::Leader);
    CHK(stateA.currentTerm() == 1);

    // Heal: A becomes reachable again.
    transport.registerNode(
        "A",
        [&](const RequestVoteArgs& args, RequestVoteReply& reply) {
            return nodeA.handleRequestVote(args, reply);
        },
        [&](const AppendEntriesArgs& args, AppendEntriesReply& reply) {
            return nodeA.handleAppendEntries(args, reply);
        });

    // B's heartbeat interval is 2: two more ticks trigger its next round,
    // which now actually reaches A.
    CHK(nodeB.tick() == Status::OK);
    CHK(nodeB.tick() == Status::OK);
    transport.pump();

    CHK(stateA.role() == Role::Follower); // Steps down: B's term (2) is newer than A's (1).
    CHK(stateA.currentTerm() == 2);
}

static void run_tests() {
    RUN(partitioned_leader_steps_down_on_heal);
}

REGISTER_TEST_SUITE();
