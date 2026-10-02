// Message Routing.
//
// Demonstrates:
// - a Transport that routes each RPC to its target node by NodeId
// - queued delivery, and replies travelling back through the callback
// - an unknown target failing immediately with a Status
// - tracing every hop, the easiest way to debug a cluster

#include <support/framework.h>

using namespace RaftCore;

namespace {

// PersistentState: the durable currentTerm and votedFor.
class MemoryPersistentState : public PersistentState {
  public:
    Status setCurrentTerm(Term term) override {
        term_ = term;
        return Status::OK;
    }
    Term currentTerm() const override {
        return term_;
    }

    Status setVotedFor(std::optional<NodeId> candidate) override {
        votedFor_ = std::move(candidate);
        return Status::OK;
    }
    std::optional<NodeId> votedFor() const override {
        return votedFor_;
    }

  private:
    Term term_ = 0;
    std::optional<NodeId> votedFor_;
};

// Storage: the log, kept as a list of terms -- payloads don't matter here.
class MemoryStorage : public Storage {
  public:
    Status append(Term term, Vector<std::uint8_t>, LogIndex& outIndex) override {
        terms_.push_back(term);
        outIndex = terms_.size();
        return Status::OK;
    }
    Status entryAt(LogIndex index, LogEntry& outEntry) const override {
        if (index == kNoIndex || index > terms_.size())
            return Status::NOT_FOUND;
        outEntry.term = terms_[index - 1];
        return Status::OK;
    }
    Status range(LogIndex fromIndex, LogIndex toIndex,
                 Vector<LogEntry>& outEntries) const override {
        if (fromIndex == kNoIndex || fromIndex > toIndex || toIndex > terms_.size())
            return Status::NOT_FOUND;
        for (LogIndex i = fromIndex; i <= toIndex; ++i) {
            LogEntry entry;
            entry.term = terms_[i - 1];
            outEntries.push_back(std::move(entry));
        }
        return Status::OK;
    }
    Status truncateFrom(LogIndex index) override {
        if (index != kNoIndex && index <= terms_.size()) {
            while (terms_.size() > index - 1)
                terms_.pop_back();
        }
        return Status::OK;
    }
    LogIndex lastIndex() const override {
        return terms_.size();
    }
    Term lastTerm() const override {
        return terms_.size() == 0 ? kNoTerm : terms_[terms_.size() - 1];
    }

  private:
    Vector<Term> terms_;
};

// RandomSource: picks each election timeout. Returning the lower bound
// makes it exact.
class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// Transport that routes RPCs between the nodes of one cluster. For each
// RPC it looks the target up by NodeId, queues a job that hands the message
// to that node, and -- when the job runs -- passes the node's reply back to
// the sender through the callback RaftNode supplied. Every hop is printed.
//
// Three properties matter for any Transport:
// - the callback is held until the reply really arrives (it's move-only,
//   so it's moved into the queued job);
// - an unknown target fails immediately, with a Status, not a reply;
// - a target that never answers just never calls the callback -- RaftCore's
//   own timeouts deal with the silence.
class Network : public Transport {
  public:
    void attach(const NodeId& id, RaftNode& node) {
        nodes_[id] = &node;
    }

    Status sendRequestVote(const NodeId& target, RequestVoteArgs args,
                           MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;

        std::cout << args.candidateId << " -> " << target << " : RequestVote, term " << args.term
                  << "\n";

        // The job's data lives behind one unique_ptr: FunctionPro requires the
        // stored callable to be nothrow-move-constructible, and a lone
        // unique_ptr always is.
        std::unique_ptr<VoteJob> job(new VoteJob{nodes_[target], target, args.candidateId,
                                                 std::move(args), std::move(onReply)});
        queue_.push_back(MoveOnlyFunction<void()>([job = std::move(job)]() {
            RequestVoteReply reply;
            if (job->node->handleRequestVote(job->args, reply) != Status::OK)
                return;
            std::cout << job->target << " -> " << job->from << " : vote "
                      << (reply.voteGranted ? "granted" : "denied") << "\n";
            job->onReply(reply);
        }));
        return Status::OK;
    }

    Status sendAppendEntries(const NodeId& target, AppendEntriesArgs args,
                             MoveOnlyFunction<void(AppendEntriesReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;

        std::cout << args.leaderId << " -> " << target << " : AppendEntries, "
                  << args.entries.size() << " entries\n";

        std::unique_ptr<AppendJob> job(new AppendJob{nodes_[target], target, args.leaderId,
                                                     std::move(args), std::move(onReply)});
        queue_.push_back(MoveOnlyFunction<void()>([job = std::move(job)]() {
            AppendEntriesReply reply;
            if (job->node->handleAppendEntries(std::move(job->args), reply) != Status::OK)
                return;
            std::cout << job->target << " -> " << job->from << " : append "
                      << (reply.success ? "ok" : "rejected") << "\n";
            job->onReply(reply);
        }));
        return Status::OK;
    }

    // How many RPCs are waiting to be delivered.
    std::size_t pending() const {
        return queue_.size() - next_;
    }

    // Delivers everything queued, including RPCs queued along the way.
    void deliverAll() {
        while (next_ < queue_.size()) {
            MoveOnlyFunction<void()> job = std::move(queue_[next_]);
            ++next_;
            job();
        }
    }

  private:
    // What a queued delivery needs, kept together behind a single pointer.
    struct VoteJob {
        RaftNode* node;
        NodeId target;
        NodeId from;
        RequestVoteArgs args;
        MoveOnlyFunction<void(RequestVoteReply)> onReply;
    };
    struct AppendJob {
        RaftNode* node;
        NodeId target;
        NodeId from;
        AppendEntriesArgs args;
        MoveOnlyFunction<void(AppendEntriesReply)> onReply;
    };

    HashMap<NodeId, RaftNode*> nodes_;
    Vector<MoveOnlyFunction<void()>> queue_;
    std::size_t next_ = 0;
};

// One cluster member: its own state, storage, and RaftNode on the shared
// Network. `timeout` is the election timeout in ticks; heartbeat is 2 ticks.
struct Member {
    Member(const NodeId& id, Vector<NodeId> peers, Network& network, RandomSource& random,
           int timeout)
        : state(persistent, storage, id),
          raft(state, network, random, std::move(peers), timeout, timeout, 2) {
        network.attach(id, raft);
    }

    MemoryPersistentState persistent;
    MemoryStorage storage;
    NodeState state;
    RaftNode raft;
};

Vector<NodeId> peersOf(const char* a, const char* b) {
    Vector<NodeId> peers;
    peers.push_back(NodeId(a));
    peers.push_back(NodeId(b));
    return peers;
}

const char* roleName(Role role) {
    switch (role) {
    case Role::Follower:
        return "Follower";
    case Role::Candidate:
        return "Candidate";
    case Role::Leader:
        return "Leader";
    }
    return "?";
}

} // namespace

static void run_examples() {
    // Nodes are identified by an opaque NodeId string, and the Transport
    // is the only thing that knows how to reach one -- here, a table from
    // NodeId to the node itself. A real Transport would map the same
    // string to an address and a socket.
    setTitle("Route by Node Id");

    Network network;
    FixedRandom random;
    Member node1("node-1", peersOf("node-2", "node-3"), network, random, 3);
    Member node2("node-2", peersOf("node-1", "node-3"), network, random, 5);
    Member node3("node-3", peersOf("node-1", "node-2"), network, random, 7);
    Member* const members[] = {&node1, &node2, &node3};

    // node-1 times out on its third tick and sends a RequestVote to each
    // peer. The Network prints each send and queues it; nothing has
    // arrived anywhere yet.
    for (int i = 0; i < 3; ++i) {
        for (Member* m : members)
            (void)m->raft.tick();
    }
    std::cout << "RPCs in flight : " << network.pending() << "\n\n"; // (2)

    // Delivering a job hands the RequestVote to the target and, if it
    // answers, sends the reply back through the callback RaftNode gave the
    // Transport. Replies can trigger more sends -- the vote that wins the
    // election makes node-1 a Leader, which immediately sends AppendEntries
    // -- and deliverAll() keeps draining until the queue is empty.
    setTitle("Deliver and Reply");

    network.deliverAll();
    std::cout << "\n";
    for (Member* m : members) {
        std::cout << m->state.selfId() << " : " << roleName(m->state.role()) << ", term "
                  << m->state.currentTerm() << "\n";
    }
    std::cout << "\n";

    // Sending to a node the Transport has never heard of is an immediate
    // dispatch failure: the send returns a Status and no callback is ever
    // called. RaftNode treats that as non-fatal -- one unreachable peer
    // doesn't stop an election -- so this is the same as a peer that never
    // replies.
    setTitle("Unknown Target");

    RequestVoteArgs args;
    args.term = 9;
    args.candidateId = "node-1";

    Status status = network.sendRequestVote(
        "node-9", args, MoveOnlyFunction<void(RequestVoteReply)>([](RequestVoteReply) {}));
    std::cout << "send to node-9 : " << (status == Status::NOT_FOUND ? "NOT_FOUND" : "sent")
              << "\n";                                             // (NOT_FOUND)
    std::cout << "RPCs in flight : " << network.pending() << "\n"; // (0: nothing was queued)
}

REGISTER_EXAMPLE_SUITE();
