// Network Partition.
//
// Demonstrates:
// - a five-node cluster split into a two-node minority (holding the old
//   Leader) and a three-node majority
// - the minority being unable to commit, while the majority elects a new
//   Leader in a higher term and commits normally
// - the partition healing: the stale Leader steps down, and its
//   uncommitted entry is discarded in favor of the majority's log
//
// The rule that decides everything is the term: a higher term wins, and
// a write is only safe once a majority of the *whole* cluster has it.

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

// Transport for one cluster: queues each RPC and delivers on deliverAll(),
// and can split the nodes into groups. An RPC only gets through if sender
// and target are in the same group; otherwise it's silently lost -- the
// sender's callback is dropped without being called, exactly like a real
// network that never answers. Nodes default to group 0.
class Network : public Transport {
  public:
    void attach(const NodeId& id, RaftNode& node) {
        nodes_[id] = &node;
    }

    void setGroup(const NodeId& id, int group) {
        group_[id] = group;
    }

    Status sendRequestVote(const NodeId& target, RequestVoteArgs args,
                           MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        if (!nodes_.contains(target))
            return Status::NOT_FOUND;
        if (!sameGroup(target, args.candidateId))
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
        if (!sameGroup(target, args.leaderId))
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
    int groupOf(const NodeId& id) {
        return group_.contains(id) ? group_[id] : 0;
    }
    bool sameGroup(const NodeId& a, const NodeId& b) {
        return groupOf(a) == groupOf(b);
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
    HashMap<NodeId, int> group_;
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

const char* const kIds[5] = {"node-1", "node-2", "node-3", "node-4", "node-5"};

// Every node except `self`.
Vector<NodeId> othersOf(const char* self) {
    Vector<NodeId> peers;
    for (const char* id : kIds) {
        if (NodeId(id) != NodeId(self))
            peers.push_back(NodeId(id));
    }
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

// Text -> bytes, for entry payloads.
Vector<std::uint8_t> toBytes(const std::string& text) {
    Vector<std::uint8_t> bytes;
    for (char c : text)
        bytes.push_back(static_cast<std::uint8_t>(c));
    return bytes;
}

// Bytes -> text, for entry payloads.
std::string toText(const Vector<std::uint8_t>& bytes) {
    std::string text;
    for (std::size_t i = 0; i < bytes.size(); ++i)
        text.push_back(static_cast<char>(bytes[i]));
    return text;
}

// One line per log entry: index, term, payload ("(no-op)" when empty --
// that's the entry a new Leader appends when it's elected).
void printLog(const NodeId& id, const Storage& log) {
    std::cout << id << " log :";
    if (log.lastIndex() == 0)
        std::cout << " (empty)";
    for (LogIndex i = 1; i <= log.lastIndex(); ++i) {
        LogEntry entry;
        (void)log.entryAt(i, entry);
        std::cout << " [" << i << ": term " << entry.term << ", "
                  << (entry.payload.size() == 0 ? "(no-op)" : "\"" + toText(entry.payload) + "\"")
                  << "]";
    }
    std::cout << "\n";
}

// Advances every node by one tick, in lockstep.
template <std::size_t N> void tickAll(Member* const (&members)[N]) {
    for (Member* m : members)
        (void)m->raft.tick();
}

template <std::size_t N> void printCluster(Member* const (&members)[N]) {
    for (const Member* m : members) {
        std::cout << m->state.selfId() << " : " << roleName(m->state.role()) << ", term "
                  << m->state.currentTerm() << ", voted for " << m->state.votedFor().value_or("-")
                  << ", log " << m->state.log().lastIndex() << ", commit " << m->state.commitIndex()
                  << "\n";
    }
    std::cout << "\n";
}

} // namespace

static void run_examples() {
    // Five nodes, each with a different election timeout (3, 5, 7, 9, and
    // 11 ticks). node-1 times out first and becomes Leader; the entry
    // "before partition" is committed on all five.
    setTitle("Five Nodes, One Leader");

    Network network;
    FixedRandom random;
    Member node1("node-1", othersOf("node-1"), network, random, 3);
    Member node2("node-2", othersOf("node-2"), network, random, 5);
    Member node3("node-3", othersOf("node-3"), network, random, 7);
    Member node4("node-4", othersOf("node-4"), network, random, 9);
    Member node5("node-5", othersOf("node-5"), network, random, 11);
    Member* const members[] = {&node1, &node2, &node3, &node4, &node5};

    for (int i = 0; i < 3; ++i)
        tickAll(members);
    network.deliverAll();

    LogIndex before = kNoIndex;
    (void)node1.storage.append(node1.state.currentTerm(), toBytes("before partition"), before);
    for (int i = 0; i < 30; ++i) {
        bool done = true;
        for (const Member* m : members)
            done = done && m->state.commitIndex() >= before;
        if (done)
            break;
        tickAll(members);
        network.deliverAll();
    }
    printCluster(members);

    // The network splits: node-1 and node-2 on one side, node-3, node-4,
    // and node-5 on the other. node-1 has no idea. It's still the Leader,
    // still heartbeats, and accepts a write -- which reaches node-2 and
    // no one else. Two of five is not a majority, so it can never commit.
    // The majority side hears nothing from node-1 and, after node-3's
    // timeout, elects node-3 in term 2 (three of five votes) -- which
    // commits its own write with node-4 and node-5.
    setTitle("Split the Network");

    network.setGroup("node-1", 0);
    network.setGroup("node-2", 0);
    network.setGroup("node-3", 1);
    network.setGroup("node-4", 1);
    network.setGroup("node-5", 1);

    LogIndex minorityIndex = kNoIndex;
    (void)node1.storage.append(node1.state.currentTerm(), toBytes("minority write"), minorityIndex);

    Member* majorityLeader = nullptr;
    for (int i = 0; i < 40 && majorityLeader == nullptr; ++i) {
        tickAll(members);
        network.deliverAll();
        for (Member* m : {&node3, &node4, &node5}) {
            if (m->state.role() == Role::Leader)
                majorityLeader = m;
        }
    }
    if (majorityLeader == nullptr)
        return;

    LogIndex majorityIndex = kNoIndex;
    (void)majorityLeader->storage.append(majorityLeader->state.currentTerm(),
                                         toBytes("majority write"), majorityIndex);
    for (int i = 0; i < 30 && majorityLeader->state.commitIndex() < majorityIndex; ++i) {
        tickAll(members);
        network.deliverAll();
    }

    printCluster(
        members); // (node-1: Leader, term 1, commit stuck. node-3: Leader, term 2, committed.)

    // Reconnect everyone. node-1's next heartbeat (term 1) is rejected by
    // the majority, whose reply carries term 2 -- so node-1 steps down. The
    // new Leader then finds where its log and node-1's diverge (right after
    // "before partition"), tells them to discard everything after that
    // point, and sends its own entries. "minority write" was never
    // committed, so discarding it breaks no promise; "majority write" was,
    // and is now everywhere.
    setTitle("Heal the Partition");

    for (const char* id : kIds)
        network.setGroup(id, 0);

    for (int i = 0; i < 60; ++i) {
        bool caughtUp = true;
        for (const Member* m : members)
            caughtUp = caughtUp && m->state.commitIndex() >= majorityIndex;
        if (caughtUp && node1.state.role() == Role::Follower &&
            node2.state.role() == Role::Follower)
            break;

        tickAll(members);
        network.deliverAll();
    }
    printCluster(members);

    printLog(node1.state.selfId(), node1.state.log()); // ("minority write" replaced)
    printLog(majorityLeader->state.selfId(), majorityLeader->state.log());
}

REGISTER_EXAMPLE_SUITE();
