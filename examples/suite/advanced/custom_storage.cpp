// Custom Storage.
//
// Demonstrates:
// - the rules a Storage implementation has to follow
// - an instrumented Storage that counts reads, appends, truncates, and
//   "syncs", showing exactly what RaftNode asks of your backend
// - how each AppendEntries case (new entries, a retry, a conflict) maps
//   onto those calls
// - a storage failure surfacing as a Status instead of a reply
//
// PersistentState follows the same rules for term and vote; the log is
// just where the most happens.

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

// Transport: sends RPCs to peers. This one accepts every RPC and drops it,
// so no reply ever comes back.
class NullTransport : public Transport {
  public:
    Status sendRequestVote(const NodeId&, RequestVoteArgs,
                           MoveOnlyFunction<void(RequestVoteReply)>) override {
        return Status::OK;
    }
    Status sendAppendEntries(const NodeId&, AppendEntriesArgs,
                             MoveOnlyFunction<void(AppendEntriesReply)>) override {
        return Status::OK;
    }
};

// RandomSource: picks each election timeout. Returning the lower bound
// makes it exact.
class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// A Storage that keeps the log in memory but behaves the way a real
// durable backend must, and counts what it's asked to do.
//
// The rules, each visible in the code below:
// - append() and truncateFrom() must be durable before they return OK. The
//   Leader counts a follower's acknowledgement toward a majority; if the
//   follower says OK and then loses the entry in a crash, a committed
//   entry could vanish. `sync()` marks the spot where a real backend would
//   fsync.
// - A failed write returns IO_ERROR and changes nothing.
// - entryAt() and range() return NOT_FOUND for indexes outside the log,
//   and leave their output untouched.
// - truncateFrom(i) discards entry i and everything after it, and is a
//   no-op (still OK) when i is past the end.
// - lastIndex() and lastTerm() can't fail -- they answer from memory.
class JournalStorage : public Storage {
  public:
    Status append(Term term, Vector<std::uint8_t> payload, LogIndex& outIndex) override {
        if (failAppends)
            return Status::IO_ERROR;

        LogEntry entry;
        entry.term = term;
        entry.payload = std::move(payload);
        entries_.push_back(std::move(entry));
        sync();

        ++appends;
        outIndex = entries_.size();
        return Status::OK;
    }

    Status entryAt(LogIndex index, LogEntry& outEntry) const override {
        ++reads;
        if (index == kNoIndex || index > entries_.size())
            return Status::NOT_FOUND;
        outEntry = entries_[index - 1];
        return Status::OK;
    }

    Status range(LogIndex fromIndex, LogIndex toIndex,
                 Vector<LogEntry>& outEntries) const override {
        ++reads;
        if (fromIndex == kNoIndex || fromIndex > toIndex || toIndex > entries_.size())
            return Status::NOT_FOUND;
        for (LogIndex i = fromIndex; i <= toIndex; ++i)
            outEntries.push_back(entries_[i - 1]);
        return Status::OK;
    }

    Status truncateFrom(LogIndex index) override {
        if (index == kNoIndex || index > entries_.size())
            return Status::OK; // Nothing to discard.

        while (entries_.size() > index - 1)
            entries_.pop_back();
        sync();

        ++truncates;
        return Status::OK;
    }

    LogIndex lastIndex() const override {
        return entries_.size();
    }
    Term lastTerm() const override {
        return entries_.size() == 0 ? kNoTerm : entries_[entries_.size() - 1].term;
    }

    // Counters, and a switch that makes appends fail.
    mutable std::size_t reads = 0;
    std::size_t appends = 0;
    std::size_t truncates = 0;
    std::size_t syncs = 0;
    bool failAppends = false;

  private:
    // Stand-in for flushing to stable storage. A real backend returns
    // only after the data would survive a power cut.
    void sync() {
        ++syncs;
    }

    Vector<LogEntry> entries_;
};

// A follower ("node-1" with two peers) built on a JournalStorage.
struct Follower {
    explicit Follower(JournalStorage& journal)
        : state(persistent, journal, "node-1"),
          raft(state, transport, random, makePeers(), 1000, 1000, 2) {}

    MemoryPersistentState persistent;
    NullTransport transport;
    FixedRandom random;
    NodeState state;
    RaftNode raft;

  private:
    static Vector<NodeId> makePeers() {
        Vector<NodeId> peers;
        peers.push_back(NodeId("node-2"));
        peers.push_back(NodeId("node-3"));
        return peers;
    }
};

// AppendEntries from leader "node-2" carrying one empty-payload entry per
// listed term, starting right after `prevIndex`.
AppendEntriesArgs makeArgs(Term term, LogIndex prevIndex, Term prevTerm,
                           std::initializer_list<Term> entryTerms) {
    AppendEntriesArgs args;
    args.term = term;
    args.leaderId = "node-2";
    args.prevLogIndex = prevIndex;
    args.prevLogTerm = prevTerm;
    for (Term entryTerm : entryTerms) {
        LogEntry entry;
        entry.term = entryTerm;
        args.entries.push_back(std::move(entry));
    }
    return args;
}

struct Counts {
    std::size_t reads, appends, truncates, syncs;
};

Counts snapshot(const JournalStorage& journal) {
    return {journal.reads, journal.appends, journal.truncates, journal.syncs};
}

// Prints how many of each operation happened since `before`.
void printDelta(const JournalStorage& journal, const Counts& before) {
    std::cout << "  reads " << journal.reads - before.reads << ", appends "
              << journal.appends - before.appends << ", truncates "
              << journal.truncates - before.truncates << ", syncs " << journal.syncs - before.syncs
              << "\n";
}

} // namespace

static void run_examples() {
    // JournalStorage (above) implements the six Storage methods and follows
    // the contract in its header comment. Everything RaftNode needs from
    // disk goes through them, so a file, an embedded database, or a
    // write-ahead log can replace it without touching the rest.
    setTitle("Plug In a Custom Storage");

    JournalStorage journal;
    Follower follower(journal);

    std::cout << "log on a new node : " << journal.lastIndex() << " entries\n\n";

    // Three new entries from the Leader. RaftNode checks whether entry 1
    // already exists (one read, NOT_FOUND) and, since it doesn't, appends
    // all three. Each append is durable before it returns, so: three
    // appends, three syncs.
    setTitle("New Entries");

    {
        const Counts before = snapshot(journal);
        AppendEntriesReply reply;
        Status status = follower.raft.handleAppendEntries(makeArgs(1, 0, 0, {1, 1, 1}), reply);
        std::cout << "AppendEntries of 3 new entries : " << (status == Status::OK ? "OK" : "error")
                  << ", success " << (reply.success ? "true" : "false") << ", log "
                  << journal.lastIndex() << "\n";
        printDelta(journal, before); // (reads 1, appends 3, truncates 0, syncs 3)
    }
    std::cout << "\n";

    // The same RPC again (the Leader retried because it never saw the
    // reply). RaftNode reads each of the three entries, finds them already
    // there with matching terms, and writes nothing. A backend that
    // re-appended here would end up with a longer, wrong log.
    setTitle("A Retried RPC");

    {
        const Counts before = snapshot(journal);
        AppendEntriesReply reply;
        Status status = follower.raft.handleAppendEntries(makeArgs(1, 0, 0, {1, 1, 1}), reply);
        std::cout << "same RPC again : " << (status == Status::OK ? "OK" : "error") << ", log "
                  << journal.lastIndex() << "\n";
        printDelta(journal, before); // (reads 3, appends 0, truncates 0, syncs 0)
    }
    std::cout << "\n";

    // A new Leader in term 2 says index 3 should have term 2, not 1. The
    // follower checks entry 2 (the previous entry), sees entry 3 conflicts,
    // truncates from there, and appends the replacement: one truncate, one
    // append, two syncs -- and truncateFrom() is the first call here that
    // shrinks the log.
    setTitle("A Conflicting Entry");

    {
        const Counts before = snapshot(journal);
        AppendEntriesReply reply;
        Status status = follower.raft.handleAppendEntries(makeArgs(2, 2, 1, {2}), reply);
        std::cout << "conflicting entry : " << (status == Status::OK ? "OK" : "error") << ", log "
                  << journal.lastIndex() << ", last term " << journal.lastTerm() << "\n";
        printDelta(journal, before); // (reads 2, appends 1, truncates 1, syncs 2)
    }
    std::cout << "\n";

    // Storage reports failure with IO_ERROR, and RaftNode passes it straight
    // up: handleAppendEntries() returns the error and sends no reply, so the
    // Leader never counts an acknowledgement for an entry that isn't there.
    // The entry is simply absent, and the Leader's next attempt retries it.
    // This is why "OK" must mean durable -- if a backend said OK and lost
    // the write, the Leader would be told something false.
    setTitle("A Failing Disk");

    journal.failAppends = true;
    {
        AppendEntriesReply reply;
        Status status = follower.raft.handleAppendEntries(makeArgs(2, 3, 2, {2}), reply);
        std::cout << "append while the disk fails : "
                  << (status == Status::IO_ERROR ? "IO_ERROR" : "other") << ", log "
                  << journal.lastIndex() << "\n"; // (IO_ERROR, log still 3)
    }

    journal.failAppends = false;
    {
        AppendEntriesReply reply;
        Status status = follower.raft.handleAppendEntries(makeArgs(2, 3, 2, {2}), reply);
        std::cout << "same RPC, disk recovered    : " << (status == Status::OK ? "OK" : "error")
                  << ", success " << (reply.success ? "true" : "false") << ", log "
                  << journal.lastIndex() << "\n"; // (OK, success true, log 4)
    }
}

REGISTER_EXAMPLE_SUITE();
