// Propose by Hand.
//
// Demonstrates:
// - RaftCore having no propose() call: a client command enters the
//   cluster by being appended to the Leader's Storage
// - refusing to propose anywhere but the Leader
// - appending versus committing, and waiting for the commit
// - a proposal on a Leader that has been cut off never committing

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

// Text -> bytes, for entry payloads.
Vector<std::uint8_t> toBytes(const std::string& text) {
    Vector<std::uint8_t> bytes;
    for (char c : text)
        bytes.push_back(static_cast<std::uint8_t>(c));
    return bytes;
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

// Appends `command` to `node`'s log as a new entry of its current term, and
// returns the new entry's index -- or nothing, if `node` isn't the Leader or
// the append failed. This is all "propose" can mean today: the entry will
// be sent to followers with the Leader's next heartbeat, and it is *not*
// committed yet.
std::optional<LogIndex> propose(Member& node, const std::string& command) {
    if (node.state.role() != Role::Leader)
        return std::nullopt;

    LogIndex index = kNoIndex;
    Status status = node.state.log().append(node.state.currentTerm(), toBytes(command), index);
    if (status != Status::OK)
        return std::nullopt;
    return index;
}

// Ticks the cluster until `leader`'s commit index reaches `index`. Returns
// how many rounds that took, or -1 if it hadn't after `maxRounds`.
template <std::size_t N>
int waitForCommit(Member& leader, Member* const (&members)[N], Network& network, LogIndex index,
                  int maxRounds) {
    for (int round = 1; round <= maxRounds; ++round) {
        tickAll(members);
        network.deliverAll();
        if (leader.state.commitIndex() >= index)
            return round;
    }
    return -1;
}

} // namespace

static void run_examples() {
    Network network;
    FixedRandom random;
    Member node1("node-1", peersOf("node-2", "node-3"), network, random, 3);
    Member node2("node-2", peersOf("node-1", "node-3"), network, random, 5);
    Member node3("node-3", peersOf("node-1", "node-2"), network, random, 7);
    Member* const members[] = {&node1, &node2, &node3};

    for (int i = 0; i < 3; ++i)
        tickAll(members);
    network.deliverAll();

    // Only the Leader may append client commands: a follower's log is
    // owned by whoever leads, and anything appended to it directly would
    // be overwritten or would corrupt the log. propose() checks the role
    // first, so a command sent to the wrong node is refused up front and
    // the caller can redirect it.
    setTitle("Only the Leader Proposes");

    std::optional<LogIndex> refused = propose(node2, "set a=1");
    std::cout << "propose on node-2 (" << roleName(node2.state.role())
              << ") : " << (refused ? "accepted" : "refused") << "\n"; // (refused)

    std::optional<LogIndex> accepted = propose(node1, "set a=1");
    std::cout << "propose on node-1 (" << roleName(node1.state.role()) << ")  : "
              << (accepted ? "accepted as entry " + std::to_string(*accepted) : "refused")
              << "\n\n"; // (accepted as entry 2: index 1 is the new Leader's no-op)

    if (!accepted)
        return;
    const LogIndex index = *accepted;

    // A successful propose only means the entry is in the Leader's own
    // log: the commit index hasn't moved and no follower has it. The entry
    // reaches followers with the next heartbeat, and is committed once a
    // majority has acknowledged it. So don't tell the client "done" yet --
    // wait for the commit.
    setTitle("Appended Is Not Committed");

    printCluster(members);

    int rounds = waitForCommit(node1, members, network, index, 30);
    std::cout << "rounds until node-1 committed entry " << index << " : " << rounds
              << "\n\n"; // (a heartbeat or two)
    printCluster(members);

    // node-1 is cut off from the others but doesn't know it: it's still a
    // Leader, so propose() happily accepts -- and the entry can never
    // reach a majority. Meanwhile the other two elect a new Leader, and
    // the entry on node-1 will eventually be thrown away (see
    // network_partition.cpp). "Accepted" never meant "safe". A caller has
    // to wait for the commit, give up after a while, and re-propose to
    // whoever is Leader now -- and make the command safe to repeat, since
    // the first attempt could still commit if the old Leader recovered.
    setTitle("A Cut-Off Leader Never Commits");

    network.setDown("node-1", true);

    std::optional<LogIndex> stranded = propose(node1, "set b=2");
    std::cout << "propose on node-1 : "
              << (stranded ? "accepted as entry " + std::to_string(*stranded) : "refused")
              << "\n"; // (accepted as entry 3)

    if (!stranded)
        return;
    int waited = waitForCommit(node1, members, network, *stranded, 15);
    std::cout << "committed within 15 rounds : " << (waited >= 0 ? "yes" : "no") << "\n\n"; // (no)
    printCluster(members); // (node-1's commit index stuck; node-2 or node-3 now Leader, term 2)
}

REGISTER_EXAMPLE_SUITE();
