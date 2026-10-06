// Integration Propose Replication Test Suite
// Verifies the client write path across a 3-node cluster: propose()
// replicates immediately instead of waiting for a heartbeat, the
// Leader applies once a majority holds the entry, and followers apply
// once a later AppendEntries tells them it committed.
//
// Covers:
// - propose() on the Leader reaches both followers' logs in a single
//   pump(), with the heartbeat interval deliberately far away
// - the Leader's StateMachine applies right after the majority ack
// - followers' StateMachines apply only after the next heartbeat
//   carries the new leaderCommit
// - a follower never applies an entry the Leader hasn't committed

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

static void propose_replicates_without_heartbeat() {
    SimulatedTransport transport;
    InMemoryStorage storageA, storageB, storageC;
    InMemoryPersistentState persistA, persistB, persistC;
    NodeState stateA(persistA, storageA, "A");
    NodeState stateB(persistB, storageB, "B");
    NodeState stateC(persistC, storageC, "C");

    ScriptedRandomSource randomA(Vector<int>{1, 1});
    ScriptedRandomSource randomB(Vector<int>{1000, 1000, 1000, 1000, 1000, 1000});
    ScriptedRandomSource randomC(Vector<int>{1000, 1000, 1000, 1000, 1000, 1000});

    // A's heartbeat interval is 50: any replication below can only come
    // from propose() itself, never from a tick().
    RaftNode nodeA(stateA, transport, randomA, Vector<NodeId>{"B", "C"}, 1, 1, 50);
    RaftNode nodeB(stateB, transport, randomB, Vector<NodeId>{"A", "C"}, 1, 1, 50);
    RaftNode nodeC(stateC, transport, randomC, Vector<NodeId>{"A", "B"}, 1, 1, 50);
    registerCluster(transport, nodeA, nodeB, nodeC);

    RecordingStateMachine machineA, machineB, machineC;
    nodeA.setStateMachine(&machineA);
    nodeB.setStateMachine(&machineB);
    nodeC.setStateMachine(&machineC);

    CHK(nodeA.tick() == Status::OK);
    transport.pump(); // votes; A becomes Leader, queues the no-op.
    CHK(stateA.role() == Role::Leader);
    transport.pump(); // no-op replicates and commits.
    CHK(stateA.commitIndex() == 1);

    LogIndex index = 0;
    CHK(nodeA.propose(makePayload("PUT a 1"), index) == Status::OK);
    CHK(index == 2);
    CHK(machineA.indices.size() == 0); // not committed yet: no ack seen.

    transport.pump(); // AppendEntries out, replies handled.

    CHK(storageB.lastIndex() == 2);
    CHK(storageC.lastIndex() == 2);
    CHK(stateA.commitIndex() == 2);

    // Leader applied as soon as the majority ack landed.
    CHK(machineA.indices.size() == 1);
    CHK(machineA.indices[0] == 2);
    CHK(machineA.payloads[0] == "PUT a 1");

    // Followers hold the entry but haven't been told it committed.
    CHK(stateB.commitIndex() == 1);
    CHK(machineB.indices.size() == 0);
    CHK(machineC.indices.size() == 0);
}

static void followers_apply_after_heartbeat() {
    SimulatedTransport transport;
    InMemoryStorage storageA, storageB, storageC;
    InMemoryPersistentState persistA, persistB, persistC;
    NodeState stateA(persistA, storageA, "A");
    NodeState stateB(persistB, storageB, "B");
    NodeState stateC(persistC, storageC, "C");

    ScriptedRandomSource randomA(Vector<int>{1, 1});
    ScriptedRandomSource randomB(Vector<int>{1000, 1000, 1000, 1000, 1000, 1000});
    ScriptedRandomSource randomC(Vector<int>{1000, 1000, 1000, 1000, 1000, 1000});

    RaftNode nodeA(stateA, transport, randomA, Vector<NodeId>{"B", "C"}, 1, 1, 1);
    RaftNode nodeB(stateB, transport, randomB, Vector<NodeId>{"A", "C"}, 1, 1, 50);
    RaftNode nodeC(stateC, transport, randomC, Vector<NodeId>{"A", "B"}, 1, 1, 50);
    registerCluster(transport, nodeA, nodeB, nodeC);

    RecordingStateMachine machineA, machineB, machineC;
    nodeA.setStateMachine(&machineA);
    nodeB.setStateMachine(&machineB);
    nodeC.setStateMachine(&machineC);

    CHK(nodeA.tick() == Status::OK);
    transport.pump();
    transport.pump();

    LogIndex index = 0;
    CHK(nodeA.propose(makePayload("PUT a 1"), index) == Status::OK);
    transport.pump();
    CHK(machineB.indices.size() == 0);

    // Heartbeat interval 1: the next tick() sends leaderCommit == 2.
    CHK(nodeA.tick() == Status::OK);
    transport.pump();

    CHK(stateB.commitIndex() == 2);
    CHK(stateC.commitIndex() == 2);
    CHK(machineB.indices.size() == 1);
    CHK(machineC.indices.size() == 1);
    CHK(machineB.payloads[0] == "PUT a 1");
    CHK(machineC.payloads[0] == "PUT a 1");
    CHK(machineB.indices[0] == 2); // no-op at 1 consumed silently.
}

static void run_tests() {
    RUN(propose_replicates_without_heartbeat);
    RUN(followers_apply_after_heartbeat);
}

REGISTER_TEST_SUITE();
