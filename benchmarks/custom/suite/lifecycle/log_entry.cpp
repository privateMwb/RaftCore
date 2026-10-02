// Log Entry Lifecycle Benchmark Suite
// Measures copy vs move of the message types that carry log data.
// Copies are not hypothetical here: replicateTo() passes its
// AppendEntriesArgs to the Transport by copy once per peer, and
// startNewElection() does the same with RequestVoteArgs.
//
// Move cases do a move-construction followed by a move-assignment back,
// so the payload buffers stay alive in one of the two objects on every
// iteration (a plain repeated move would leave the source empty after
// the first call and stop measuring anything). Each iteration is
// therefore two moves.
//
// Covers:
// - LogEntry copy at 0 and 256 payload bytes, and move at 256
// - AppendEntriesArgs copy at 1, 10 and 100 entries (64-byte
//   payloads), and move at 100
// - RequestVoteArgs copy

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Builds a LogEntry with `payloadBytes` bytes of payload.
static LogEntry makeEntry(std::size_t payloadBytes) {
    LogEntry entry;
    entry.term = 7;
    for (std::size_t i = 0; i < payloadBytes; ++i)
        entry.payload.push_back(static_cast<std::uint8_t>(i));
    return entry;
}

// Builds AppendEntries args carrying `entryCount` entries of 64-byte payloads.
static AppendEntriesArgs makeArgs(std::size_t entryCount) {
    AppendEntriesArgs args;
    args.term = 7;
    args.leaderId = "node-1";
    args.prevLogIndex = 10;
    args.prevLogTerm = 7;
    args.leaderCommit = 10;
    for (std::size_t i = 0; i < entryCount; ++i)
        args.entries.push_back(makeEntry(64));
    return args;
}

// Measures copying a LogEntry with an empty payload (a no-op entry).
static void bench_entry_copy_empty() {
    const LogEntry src = makeEntry(0);

    auto copy = [&] {
        LogEntry c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("entry cp 0", copy);
}

// Measures copying a LogEntry with a 256-byte payload.
static void bench_entry_copy() {
    const LogEntry src = makeEntry(256);

    auto copy = [&] {
        LogEntry c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("entry cp 256", copy);
}

// Measures moving a LogEntry with a 256-byte payload (move-construct,
// then move-assign back).
static void bench_entry_move() {
    LogEntry a = makeEntry(256);

    auto move = [&] {
        LogEntry tmp(std::move(a));
        a = std::move(tmp);
        doNotOptimize(a);
    };
    BENCH_SOLO("entry mv 256", move);
}

// Measures copying AppendEntriesArgs holding 1 entry.
static void bench_args_copy_1() {
    const AppendEntriesArgs src = makeArgs(1);

    auto copy = [&] {
        AppendEntriesArgs c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("args cp 1", copy);
}

// Measures copying AppendEntriesArgs holding 10 entries.
static void bench_args_copy_10() {
    const AppendEntriesArgs src = makeArgs(10);

    auto copy = [&] {
        AppendEntriesArgs c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("args cp 10", copy);
}

// Measures copying AppendEntriesArgs holding 100 entries.
static void bench_args_copy_100() {
    const AppendEntriesArgs src = makeArgs(100);

    auto copy = [&] {
        AppendEntriesArgs c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("args cp 100", copy);
}

// Measures moving AppendEntriesArgs holding 100 entries (move-construct,
// then move-assign back) -- the contrast to "args cp 100".
static void bench_args_move_100() {
    AppendEntriesArgs a = makeArgs(100);

    auto move = [&] {
        AppendEntriesArgs tmp(std::move(a));
        a = std::move(tmp);
        doNotOptimize(a);
    };
    BENCH_SOLO("args mv 100", move);
}

// Measures copying RequestVoteArgs (a NodeId string plus scalars).
static void bench_vote_args_copy() {
    RequestVoteArgs src;
    src.term = 8;
    src.candidateId = "node-1";
    src.lastLogIndex = 10;
    src.lastLogTerm = 7;

    auto copy = [&] {
        RequestVoteArgs c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("vote cp", copy);
}

// Executes all log-entry and message lifecycle benchmark cases.
static void run_benchmarks() {
    bench_entry_copy_empty();
    bench_entry_copy();
    bench_entry_move();
    bench_args_copy_1();
    bench_args_copy_10();
    bench_args_copy_100();
    bench_args_move_100();
    bench_vote_args_copy();
}

REGISTER_BENCH_SUITE();
