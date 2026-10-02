// Crash Recovery.
//
// Demonstrates:
// - splitting a node into what survives a crash (PersistentState and
//   Storage: term, vote, log) and what doesn't (NodeState, RaftNode: role,
//   commit index, timers)
// - restarting a node by building a new NodeState and RaftNode over the
//   same PersistentState and Storage
// - the restarted node coming back as a Follower, catching up from the
//   Leader, and relearning the commit index
// - why the persisted vote matters: a restarted node still refuses a
//   second vote in the same term

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

// Storage: the log. Whole entries are kept, payload included, so the
// example can read back what was replicated.
class MemoryStorage : public Storage {
  public:
    Status append(Term term, Vector<std::uint8_t> payload, LogIndex& outIndex) override {
        LogEntry entry;
        entry.term = term;
        entry.payload = std::move(payload);
        entries_.push_back(std::move(entry));
        outIndex = entries_.size();
        return Status::OK;
    }
    Status entryAt(LogIndex index, LogEntry& outEntry) const override {
        if (index == kNoIndex || index > entries_.size())
            return Status::NOT_FOUND;
        outEntry = entries_[index - 1];
        return Status::OK;
    }
    Status range(LogIndex fromIndex, LogIndex toIndex,
                 Vector<LogEntry>& outEntries) const override {
        if (fromIndex == kNoIndex || fromIndex > toIndex || toIndex > entries_.size())
            return Status::NOT_FOUND;
        for (LogIndex i = fromIndex; i <= toIndex; ++i)
            outEntries.push_back(entries_[i - 1]);
        return Status::OK;
    }
    Status truncateFrom(LogIndex index) override {
        if (index != kNoIndex && index <= entries_.size()) {
            while (entries_.size() > index - 1)
                entries_.pop_back();
        }
        return Status::OK;
    }
    LogIndex lastIndex() const override {
        return entries_.size();
    }
    Term lastTerm() const override {
        return entries_.size() == 0 ? kNoTerm : entries_[entries_.size() - 1].term;
    }

  private:
    Vector<LogEntry> entries_;
};

// RandomSource: picks each election timeout. Returning the lower bound
// makes it exact -- and each node below gets its own bound.
class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// Transport for one cluster: queues each RPC, delivers on deliverAll(), and
// can take a node "down". While a node is down, RPCs to it and RPCs it
// sends are silently lost -- the sender's callback is dropped without being
// called, exactly like a real network that never answers. RaftCore's own
// timeouts do the noticing.
class Network : public Transport {
  public:
    void attach(const NodeId& id, RaftNode& node) {
        nodes_[id] = &node;
    }

    void setDown(const NodeId& id, bool down) {
        down_[id] = down;
    }

    Status sendRequestVote(const NodeId& target, RequestVoteArgs args,
                           MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;
        if (isDown(target) || isDown(args.candidateId))
            return Status::OK; // Lost in transit.

        // The job's data lives behind one unique_ptr: FunctionPro requires the
        // stored callable to be nothrow-move-constructible, and a lone
        // unique_ptr always is.
        std::unique_ptr<VoteJob> job(
            new VoteJob{nodes_[target], std::move(args), std::move(onReply)});
        queue_.push_back(MoveOnlyFunction<void()>([job = std::move(job)]() {
            RequestVoteReply reply;
            if (job->node->handleRequestVote(job->args, reply) != Status::OK)
                return;
            job->onReply(reply);
        }));
        return Status::OK;
    }

    Status sendAppendEntries(const NodeId& target, AppendEntriesArgs args,
                             MoveOnlyFunction<void(AppendEntriesReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;
        if (isDown(target) || isDown(args.leaderId))
            return Status::OK; // Lost in transit.

        std::unique_ptr<AppendJob> job(
            new AppendJob{nodes_[target], std::move(args), std::move(onReply)});
        queue_.push_back(MoveOnlyFunction<void()>([job = std::move(job)]() {
            AppendEntriesReply reply;
            if (job->node->handleAppendEntries(std::move(job->args), reply) != Status::OK)
                return;
            job->onReply(reply);
        }));
        return Status::OK;
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
    bool isDown(const NodeId& id) {
        return down_.contains(id) && down_[id];
    }

    // What a queued delivery needs, kept together behind a single pointer.
    struct VoteJob {
        RaftNode* node;
        RequestVoteArgs args;
        MoveOnlyFunction<void(RequestVoteReply)> onReply;
    };
    struct AppendJob {
        RaftNode* node;
        AppendEntriesArgs args;
        MoveOnlyFunction<void(AppendEntriesReply)> onReply;
    };

    HashMap<NodeId, RaftNode*> nodes_;
    HashMap<NodeId, bool> down_;
    Vector<MoveOnlyFunction<void()>> queue_;
    std::size_t next_ = 0;
};

// The durable half of a node: what a real deployment writes to disk. It
// outlives the process, so it's owned separately.
struct Durable {
    MemoryPersistentState persistent;
    MemoryStorage storage;
};

// The volatile half: NodeState and RaftNode. Destroying one is a crash;
// building a new one over the same Durable is a restart.
struct Process {
    Process(const NodeId& id, Vector<NodeId> peers, Durable& durable, Network& network,
            RandomSource& random, int timeout)
        : state(durable.persistent, durable.storage, id),
          raft(state, network, random, std::move(peers), timeout, timeout, 2) {
        network.attach(id, raft);
    }

    NodeState state;
    RaftNode raft;
};

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

// Text -> bytes, for entry payloads.
Vector<std::uint8_t> toBytes(const std::string& text) {
    Vector<std::uint8_t> bytes;
    for (char c : text)
        bytes.push_back(static_cast<std::uint8_t>(c));
    return bytes;
}

} // namespace

static void run_examples() {
    Network network;
    FixedRandom random;

    const char* ids[3] = {"node-1", "node-2", "node-3"};
    const int timeouts[3] = {3, 5, 7};

    Durable durable[3];
    std::unique_ptr<Process> process[3];

    // (Re)starts node `i` over its Durable, peering with the other two.
    auto start = [&](int i) {
        Vector<NodeId> peers;
        for (int j = 0; j < 3; ++j) {
            if (j != i)
                peers.push_back(NodeId(ids[j]));
        }
        process[i].reset(
            new Process(ids[i], std::move(peers), durable[i], network, random, timeouts[i]));
    };

    auto tickAll = [&] {
        for (auto& p : process) {
            if (p)
                (void)p->raft.tick();
        }
        network.deliverAll();
    };

    auto show = [&](int i) {
        const NodeState& s = process[i]->state;
        std::cout << ids[i] << " : " << roleName(s.role()) << ", term " << s.currentTerm()
                  << ", voted for " << s.votedFor().value_or("-") << ", log " << s.log().lastIndex()
                  << ", commit " << s.commitIndex() << "\n";
    };

    // Each node is a Durable (term, vote, log) plus a Process (everything
    // else). Elect node-1 and commit two commands on all three nodes.
    setTitle("Run a Cluster on Durable State");

    for (int i = 0; i < 3; ++i)
        start(i);
    for (int i = 0; i < 3; ++i)
        tickAll();

    LogIndex last = kNoIndex;
    (void)process[0]->state.log().append(process[0]->state.currentTerm(), toBytes("set a=1"), last);
    (void)process[0]->state.log().append(process[0]->state.currentTerm(), toBytes("set b=2"), last);
    for (int i = 0; i < 30; ++i) {
        bool done = true;
        for (auto& p : process)
            done = done && p->state.commitIndex() >= last;
        if (done)
            break;
        tickAll();
    }
    for (int i = 0; i < 3; ++i)
        show(i);
    std::cout << "\n";

    // Crashing node-2 destroys its NodeState and RaftNode -- role, commit
    // index, election timer, all of it. What's left is exactly what a
    // real node would find on disk: read straight from the Durable.
    // (The Network is drained first and told node-2 is down, so nothing
    // is delivered to the destroyed node.)
    setTitle("Crash a Follower");

    network.deliverAll();
    network.setDown("node-2", true);
    process[1].reset();

    std::cout << "node-2 on disk : term " << durable[1].persistent.currentTerm() << ", voted for "
              << durable[1].persistent.votedFor().value_or("-") << ", log "
              << durable[1].storage.lastIndex() << "\n\n"; // (term 1, voted for node-1, log 3)

    // While node-2 is down the rest of the cluster keeps working: node-1 is
    // still Leader, and node-3 plus the Leader are a majority. One more
    // command commits without node-2.
    LogIndex missed = kNoIndex;
    (void)process[0]->state.log().append(process[0]->state.currentTerm(), toBytes("set c=3"),
                                         missed);
    for (int i = 0; i < 30 && (process[0]->state.commitIndex() < missed ||
                               process[2]->state.commitIndex() < missed);
         ++i)
        tickAll();

    // A new NodeState reads term, vote, and log back from the Durable, so
    // node-2 remembers all three. Everything else starts from scratch:
    // every node restarts as a Follower, and commit index starts at 0 --
    // it is volatile, and the Leader will tell it again.
    setTitle("Restart From Durable State");

    start(1);
    network.setDown("node-2", false);
    std::cout << "just restarted : ";
    show(1); // (Follower, term 1, voted for node-1, log 3, commit 0)

    for (int i = 0; i < 30 && process[1]->state.commitIndex() < missed; ++i)
        tickAll();
    std::cout << "after catch-up : ";
    show(1); // (log 4, commit 4: it received the entry it missed)
    std::cout << "\n";

    // node-2 voted for node-1 in term 1, and that vote was written to the
    // PersistentState before the vote was granted. So when node-3 now asks
    // for node-2's vote in the same term, the restarted node refuses. A node
    // that forgot its vote across a restart could grant it twice and let two
    // candidates each collect a majority in one term.
    setTitle("The Vote Survives the Restart");

    RequestVoteArgs args;
    args.term = process[1]->state.currentTerm();
    args.candidateId = "node-3";
    args.lastLogIndex = process[1]->state.log().lastIndex();
    args.lastLogTerm = process[1]->state.log().lastTerm();

    RequestVoteReply reply;
    Status status = process[1]->raft.handleRequestVote(args, reply);
    std::cout << "handler returned : " << (status == Status::OK ? "OK" : "error") << "\n";
    std::cout << "vote for node-3  : " << (reply.voteGranted ? "granted" : "denied")
              << "\n"; // (denied: already voted for node-1 in this term)
}

REGISTER_EXAMPLE_SUITE();
