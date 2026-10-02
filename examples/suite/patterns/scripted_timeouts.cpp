// Scripted Timeouts.
//
// Demonstrates:
// - RandomSource as the only source of election-timeout randomness
// - a scripted RandomSource that hands back chosen timeouts
// - deciding, in advance, which node times out first and wins
// - the same cluster and code producing a different winner from a
//   different script

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

// RandomSource that hands back a fixed list of values in order, repeating,
// ignoring the requested range. RaftNode asks for a timeout when it's
// constructed and again every time its election timer resets, so a
// one-value script means "this node always times out after N ticks".
class ScriptedRandom : public RandomSource {
  public:
    void add(int value) {
        values_.push_back(value);
    }

    int nextInt(int, int) override {
        return values_[next_++ % values_.size()];
    }

  private:
    Vector<int> values_;
    std::size_t next_ = 0;
};

// Transport that routes RPCs between the nodes of one cluster (see
// message_routing.cpp for the annotated version). For each
// RPC it looks the target up by NodeId, queues a job that hands the message
// to that node, and -- when the job runs -- passes the node's reply back to
// the sender through the callback RaftNode supplied.
class Network : public Transport {
  public:
    void attach(const NodeId& id, RaftNode& node) {
        nodes_[id] = &node;
    }

    Status sendRequestVote(const NodeId& target, RequestVoteArgs args,
                           MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;

        // The job's data lives behind one unique_ptr: FunctionPro requires the
        // stored callable to be nothrow-move-constructible, and a lone
        // unique_ptr always is.
        std::unique_ptr<VoteJob> job(
            new VoteJob{nodes_[target], target, std::move(args), std::move(onReply)});
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

        std::unique_ptr<AppendJob> job(
            new AppendJob{nodes_[target], target, std::move(args), std::move(onReply)});
        queue_.push_back(MoveOnlyFunction<void()>([job = std::move(job)]() {
            AppendEntriesReply reply;
            if (job->node->handleAppendEntries(std::move(job->args), reply) != Status::OK)
                return;
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
        RequestVoteArgs args;
        MoveOnlyFunction<void(RequestVoteReply)> onReply;
    };
    struct AppendJob {
        RaftNode* node;
        NodeId target;
        AppendEntriesArgs args;
        MoveOnlyFunction<void(AppendEntriesReply)> onReply;
    };

    HashMap<NodeId, RaftNode*> nodes_;
    Vector<MoveOnlyFunction<void()>> queue_;
    std::size_t next_ = 0;
};

// One cluster member with its own scripted timeouts. The script overrides
// the (1 to 100 tick) range the RaftNode is given, so keep script values
// inside it.
struct Member {
    Member(const NodeId& id, Vector<NodeId> peers, Network& network, int timeoutTicks)
        : state(persistent, storage, id),
          raft(state, network, scripted(timeoutTicks), std::move(peers), 1, 100, 2) {
        network.attach(id, raft);
    }

    MemoryPersistentState persistent;
    MemoryStorage storage;
    ScriptedRandom random;
    NodeState state;
    RaftNode raft;

  private:
    // Fills this member's script, then hands it to RaftNode. Runs while the
    // member is being constructed, before RaftNode asks for its first timeout.
    ScriptedRandom& scripted(int timeoutTicks) {
        random.add(timeoutTicks);
        return random;
    }
};

Vector<NodeId> peersOf(const char* a, const char* b) {
    Vector<NodeId> peers;
    peers.push_back(NodeId(a));
    peers.push_back(NodeId(b));
    return peers;
}

// Builds a fresh three-node cluster whose nodes time out after t1, t2, and
// t3 ticks, ticks them together until one starts an election, delivers the
// votes, and reports who was first and who won.
void electWith(int t1, int t2, int t3) {
    Network network;
    Member node1("node-1", peersOf("node-2", "node-3"), network, t1);
    Member node2("node-2", peersOf("node-1", "node-3"), network, t2);
    Member node3("node-3", peersOf("node-1", "node-2"), network, t3);
    Member* const members[] = {&node1, &node2, &node3};

    int ticks = 0;
    const Member* first = nullptr;
    while (first == nullptr && ticks < 20) {
        for (Member* m : members)
            (void)m->raft.tick();
        ++ticks;
        for (const Member* m : members) {
            if (first == nullptr && m->state.role() != Role::Follower)
                first = m;
        }
    }
    if (first != nullptr)
        std::cout << "first to time out : " << first->state.selfId() << " after " << ticks
                  << " ticks\n";

    network.deliverAll();

    for (const Member* m : members) {
        if (m->state.role() == Role::Leader)
            std::cout << "leader            : " << m->state.selfId() << ", term "
                      << m->state.currentTerm() << "\n";
    }
    std::cout << "\n";
}

} // namespace

static void run_examples() {
    // A real cluster uses SystemRandomSource, which picks each timeout at
    // random from the configured range so nodes rarely time out together.
    // That's right for production and useless for a repeatable example, so
    // each node here gets a ScriptedRandom instead: one value, meaning
    // "always time out after N ticks". Whoever has the smallest number
    // times out first.
    setTitle("Script Each Node's Timeout");

    std::cout << "run 1 : node-1 = 9 ticks, node-2 = 4 ticks, node-3 = 6 ticks\n";
    std::cout << "run 2 : node-1 = 6 ticks, node-2 = 9 ticks, node-3 = 4 ticks\n\n";

    // node-2 has the shortest timeout, so it must time out first, collect
    // votes from the others (who haven't voted this term), and win.
    setTitle("Predict the Winner");

    std::cout << "run 1\n";
    electWith(9, 4, 6); // (node-2 after 4 ticks)

    // Same code, same cluster, different numbers: now node-3 is the
    // quickest, and it wins instead. This is how a test forces a specific
    // election outcome without sleeping or hoping for luck.
    setTitle("Change the Script, Change the Winner");

    std::cout << "run 2\n";
    electWith(6, 9, 4); // (node-3 after 4 ticks)
}

REGISTER_EXAMPLE_SUITE();
