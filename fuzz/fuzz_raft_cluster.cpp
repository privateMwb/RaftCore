// ============================================================
// fuzz/fuzz_raft_cluster.cpp
//
// Safety-property fuzzer for a whole three-node RaftCore cluster.
//
// Unlike fuzz_raft_rpc.cpp, the nodes here generate their own RPCs, so
// every message is protocol-conformant and the real Raft safety
// properties (Figure 3 of the Raft paper) must hold no matter what the
// fuzzer does to the schedule. The fuzzer's bytes control only the
// environment:
//
//   - which node ticks (so elections and timeouts happen in any order)
//   - which queued RPC is delivered next (so messages are reordered)
//   - which queued RPC is dropped (message loss)
//   - a network partition toggled on or off for a node
//   - a client command appended on whichever node currently believes
//     it is Leader, including stale Leaders
//   - a node crashing and restarting from its PersistentState and
//     Storage only (volatile state -- role, commit index, timers --
//     is lost, exactly as in a real restart)
//
// After EVERY operation the harness checks:
//
//   Election Safety      at most one Leader is ever observed per term
//   Term monotonicity    a node's currentTerm never decreases, even
//                        across a restart
//   Vote stability       votedFor never changes or clears within a term
//   Log Matching         if two logs hold an entry with the same index
//                        and term, they are identical up to that index
//   State Machine Safety every entry any node has committed is the same
//                        entry (term and payload) on every node that has
//                        committed that index, now and in the future
//   Commit sanity        commitIndex <= lastIndex, and never decreases
//                        within one run of a node
//
// A failure aborts, so libFuzzer's reproducer is the exact schedule
// that broke the invariant.
//
// Deliberately NOT covered yet: duplicated messages (the reply
// callbacks are move-only, so a message can be lost or delayed but not
// delivered twice), clusters of other sizes, storage write failures,
// and anything involving membership change or snapshots (RaftCore has
// neither).
// ============================================================

#include <RaftCore/RaftCore.h>

#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <vector>

using namespace RaftCore;

namespace {

void require(bool ok) {
    if (!ok)
        std::abort();
}

// Reads fuzzer bytes one at a time; yields 0 once the input runs out.
class ByteReader {
  public:
    ByteReader(const std::uint8_t* data, std::size_t size) : data_(data), size_(size) {}

    bool done() const {
        return pos_ >= size_;
    }
    std::uint8_t next() {
        return pos_ < size_ ? data_[pos_++] : 0;
    }

  private:
    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t pos_ = 0;
};

const char* const kIds[3] = {"node-1", "node-2", "node-3"};

int indexOf(const NodeId& id) {
    for (int i = 0; i < 3; ++i) {
        if (id == kIds[i])
            return i;
    }
    return -1;
}

// In-memory PersistentState: the durable currentTerm and votedFor.
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

// In-memory Storage: the log, with payloads.
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

class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// Transport that queues every RPC so the fuzzer decides what happens to
// it: delivered (in any order), dropped, or lost to a partition. A
// queued RPC records who sent it and who it's for; when a node crashes,
// everything involving it is discarded, because the reply callbacks the
// sender's RaftNode handed over point at that RaftNode and must never
// outlive it.
class FuzzNetwork : public Transport {
  public:
    void attach(int index, RaftNode* node) {
        nodes_[index] = node;
    }
    void setGroup(int index, int group) {
        group_[index] = group;
    }
    int group(int index) const {
        return group_[index];
    }
    std::size_t pending() const {
        return queue_.size();
    }

    Status sendRequestVote(const NodeId& target, RequestVoteArgs args,
                           MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        const int from = indexOf(args.candidateId);
        const int to = indexOf(target);
        if (from < 0 || to < 0)
            return Status::NOT_FOUND;

        // The closure holds one unique_ptr (plus plain values): FunctionPro
        // requires the stored callable to be nothrow-move-constructible.
        std::unique_ptr<VoteJob> job(new VoteJob{std::move(args), std::move(onReply)});
        enqueue(from, to, MoveOnlyFunction<void()>([this, to, job = std::move(job)]() {
                    RaftNode* node = nodes_[to];
                    if (node == nullptr)
                        return;
                    RequestVoteReply reply;
                    if (node->handleRequestVote(job->args, reply) != Status::OK)
                        return;
                    job->onReply(reply);
                }));
        return Status::OK;
    }

    Status sendAppendEntries(const NodeId& target, AppendEntriesArgs args,
                             MoveOnlyFunction<void(AppendEntriesReply)> onReply) override {
        const int from = indexOf(args.leaderId);
        const int to = indexOf(target);
        if (from < 0 || to < 0)
            return Status::NOT_FOUND;

        std::unique_ptr<AppendJob> job(new AppendJob{std::move(args), std::move(onReply)});
        enqueue(from, to, MoveOnlyFunction<void()>([this, to, job = std::move(job)]() {
                    RaftNode* node = nodes_[to];
                    if (node == nullptr)
                        return;
                    AppendEntriesReply reply;
                    if (node->handleAppendEntries(std::move(job->args), reply) != Status::OK)
                        return;
                    job->onReply(reply);
                }));
        return Status::OK;
    }

    // Delivers queued RPC number `k` (modulo the queue length) -- unless
    // sender and target are on opposite sides of a partition, in which
    // case it's lost.
    void deliver(std::size_t k) {
        if (queue_.empty())
            return;
        k %= queue_.size();
        std::unique_ptr<Pending> pending = std::move(queue_[k]);
        queue_.erase(queue_.begin() + static_cast<std::ptrdiff_t>(k));
        if (group_[pending->from] != group_[pending->to])
            return;
        pending->run();
    }

    // Discards queued RPC number `k` (modulo the queue length).
    void drop(std::size_t k) {
        if (queue_.empty())
            return;
        k %= queue_.size();
        queue_.erase(queue_.begin() + static_cast<std::ptrdiff_t>(k));
    }

    // Discards every queued RPC sent by or addressed to node `index`.
    void forget(int index) {
        for (std::size_t i = 0; i < queue_.size();) {
            if (queue_[i]->from == index || queue_[i]->to == index)
                queue_.erase(queue_.begin() + static_cast<std::ptrdiff_t>(i));
            else
                ++i;
        }
    }

  private:
    struct VoteJob {
        RequestVoteArgs args;
        MoveOnlyFunction<void(RequestVoteReply)> onReply;
    };
    struct AppendJob {
        AppendEntriesArgs args;
        MoveOnlyFunction<void(AppendEntriesReply)> onReply;
    };
    struct Pending {
        int from;
        int to;
        MoveOnlyFunction<void()> run;
    };

    void enqueue(int from, int to, MoveOnlyFunction<void()> run) {
        // Bound the queue so a long input can't make it grow without limit.
        if (queue_.size() >= 256)
            queue_.erase(queue_.begin());
        queue_.emplace_back(new Pending{from, to, std::move(run)});
    }

    RaftNode* nodes_[3] = {nullptr, nullptr, nullptr};
    int group_[3] = {0, 0, 0};
    std::vector<std::unique_ptr<Pending>> queue_;
};

// What survives a crash: a node's PersistentState and Storage.
struct Durable {
    MemoryPersistentState persistent;
    MemoryStorage storage;
};

Vector<NodeId> peersOf(int index) {
    Vector<NodeId> peers;
    for (int i = 0; i < 3; ++i) {
        if (i != index)
            peers.push_back(NodeId(kIds[i]));
    }
    return peers;
}

// What doesn't: NodeState and RaftNode. Destroying one is a crash.
struct Process {
    Process(int index, Durable& durable, FuzzNetwork& network, RandomSource& random, int timeout)
        : state(durable.persistent, durable.storage, kIds[index]),
          raft(state, network, random, peersOf(index), timeout, timeout, 2) {
        network.attach(index, &raft);
    }

    NodeState state;
    RaftNode raft;
};

bool sameEntry(const LogEntry& a, const LogEntry& b) {
    if (a.term != b.term || a.payload.size() != b.payload.size())
        return false;
    for (std::size_t k = 0; k < a.payload.size(); ++k) {
        if (a.payload[k] != b.payload[k])
            return false;
    }
    return true;
}

// Reads entry `index` of `state`'s log; the caller has already checked
// it's in range.
LogEntry entryOf(const NodeState& state, LogIndex index) {
    LogEntry entry;
    require(state.log().entryAt(index, entry) == Status::OK);
    return entry;
}

// Everything the checker remembers between operations.
struct Tracker {
    Term lastTerm[3] = {0, 0, 0};
    LogIndex lastCommit[3] = {0, 0, 0};
    std::optional<NodeId> lastVote[3];
    std::map<Term, int> leaderOfTerm;
    std::vector<LogEntry> committed; // committed[i - 1] is the entry committed at index i.
};

void checkInvariants(const std::unique_ptr<Process> (&process)[3], Tracker& tracker) {
    for (int i = 0; i < 3; ++i) {
        const NodeState& state = process[i]->state;
        const Term term = state.currentTerm();
        const std::optional<NodeId> vote = state.votedFor();

        // Term monotonicity, including across restarts.
        require(term >= tracker.lastTerm[i]);

        // Vote stability: once cast in a term, it stays.
        if (term == tracker.lastTerm[i] && tracker.lastVote[i].has_value())
            require(vote.has_value() && *vote == *tracker.lastVote[i]);

        // Commit sanity.
        require(state.commitIndex() >= tracker.lastCommit[i]);
        require(state.commitIndex() <= state.log().lastIndex());

        // Election Safety: one Leader per term.
        if (state.role() == Role::Leader) {
            const auto it = tracker.leaderOfTerm.find(term);
            if (it == tracker.leaderOfTerm.end())
                tracker.leaderOfTerm[term] = i;
            else
                require(it->second == i);
        }

        // State Machine Safety: every committed entry is the same entry
        // on every node, and stays the same over time.
        for (LogIndex index = 1; index <= state.commitIndex(); ++index) {
            const LogEntry entry = entryOf(state, index);
            if (index > tracker.committed.size())
                tracker.committed.push_back(entry);
            else
                require(sameEntry(entry, tracker.committed[index - 1]));
        }

        tracker.lastTerm[i] = term;
        tracker.lastCommit[i] = state.commitIndex();
        tracker.lastVote[i] = vote;
    }

    // Log Matching: same index and term implies the same entry and the
    // same history before it.
    for (int a = 0; a < 3; ++a) {
        for (int b = a + 1; b < 3; ++b) {
            const NodeState& sa = process[a]->state;
            const NodeState& sb = process[b]->state;
            const LogIndex common = sa.log().lastIndex() < sb.log().lastIndex()
                                        ? sa.log().lastIndex()
                                        : sb.log().lastIndex();
            bool matchedAbove = false;
            for (LogIndex index = common; index >= 1; --index) {
                const LogEntry ea = entryOf(sa, index);
                const LogEntry eb = entryOf(sb, index);
                if (ea.term == eb.term) {
                    require(sameEntry(ea, eb));
                    matchedAbove = true;
                } else {
                    require(!matchedAbove);
                }
            }
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size == 0)
        return 0;

    ByteReader in(data, size);

    // The first byte picks the election timeouts: different per node (a
    // clean election), identical (a split-vote livelock), or reversed.
    static const int kTimeouts[4][3] = {{3, 5, 7}, {3, 3, 3}, {2, 4, 6}, {6, 2, 4}};
    const int* timeouts = kTimeouts[in.next() % 4];

    FuzzNetwork network;
    FixedRandom random;
    Durable durable[3];
    std::unique_ptr<Process> process[3];
    Tracker tracker;

    auto start = [&](int i) {
        process[i].reset(new Process(i, durable[i], network, random, timeouts[i]));
    };
    for (int i = 0; i < 3; ++i)
        start(i);

    while (!in.done()) {
        switch (in.next() % 8) {
        case 0:
        case 1: { // tick one node
            (void)process[in.next() % 3]->raft.tick();
            break;
        }
        case 2:
        case 3: { // deliver a queued RPC (any order)
            network.deliver(in.next());
            break;
        }
        case 4: { // drop a queued RPC
            network.drop(in.next());
            break;
        }
        case 5: { // client command on whichever node thinks it's Leader
            const int i = in.next() % 3;
            const std::uint8_t byte = in.next();
            NodeState& state = process[i]->state;
            if (state.role() == Role::Leader) {
                Vector<std::uint8_t> payload;
                payload.push_back(byte);
                LogIndex index = kNoIndex;
                (void)state.log().append(state.currentTerm(), std::move(payload), index);
            }
            break;
        }
        case 6: { // crash and restart a node from its durable state
            const int i = in.next() % 3;
            network.attach(i, nullptr);
            network.forget(i);
            process[i].reset();
            start(i);
            tracker.lastCommit[i] = 0; // Commit index is volatile.
            break;
        }
        case 7: { // move a node to one side of a partition or the other
            const int i = in.next() % 3;
            network.setGroup(i, in.next() & 1);
            break;
        }
        default:
            break;
        }

        checkInvariants(process, tracker);
    }

    return 0;
}
