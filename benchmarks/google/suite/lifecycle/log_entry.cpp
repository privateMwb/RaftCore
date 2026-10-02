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

#include <benchmark/benchmark.h>

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
static void log_entry_copy_0(benchmark::State& state) {
    const LogEntry src = makeEntry(0);

    for (auto _ : state) {
        LogEntry c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(log_entry_copy_0);

// Measures copying a LogEntry with a 256-byte payload.
static void log_entry_copy_256(benchmark::State& state) {
    const LogEntry src = makeEntry(256);

    for (auto _ : state) {
        LogEntry c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(log_entry_copy_256);

// Measures moving a LogEntry with a 256-byte payload (move-construct,
// then move-assign back).
static void log_entry_move_256(benchmark::State& state) {
    LogEntry a = makeEntry(256);

    for (auto _ : state) {
        LogEntry tmp(std::move(a));
        a = std::move(tmp);
        benchmark::DoNotOptimize(a);
    }
}
BENCHMARK(log_entry_move_256);

// Measures copying AppendEntriesArgs holding 1 entry.
static void log_entry_args_copy_1(benchmark::State& state) {
    const AppendEntriesArgs src = makeArgs(1);

    for (auto _ : state) {
        AppendEntriesArgs c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(log_entry_args_copy_1);

// Measures copying AppendEntriesArgs holding 10 entries.
static void log_entry_args_copy_10(benchmark::State& state) {
    const AppendEntriesArgs src = makeArgs(10);

    for (auto _ : state) {
        AppendEntriesArgs c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(log_entry_args_copy_10);

// Measures copying AppendEntriesArgs holding 100 entries.
static void log_entry_args_copy_100(benchmark::State& state) {
    const AppendEntriesArgs src = makeArgs(100);

    for (auto _ : state) {
        AppendEntriesArgs c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(log_entry_args_copy_100);

// Measures moving AppendEntriesArgs holding 100 entries (move-construct,
// then move-assign back) -- the contrast to log_entry_args_copy_100.
static void log_entry_args_move_100(benchmark::State& state) {
    AppendEntriesArgs a = makeArgs(100);

    for (auto _ : state) {
        AppendEntriesArgs tmp(std::move(a));
        a = std::move(tmp);
        benchmark::DoNotOptimize(a);
    }
}
BENCHMARK(log_entry_args_move_100);

// Measures copying RequestVoteArgs (a NodeId string plus scalars).
static void log_entry_vote_copy(benchmark::State& state) {
    RequestVoteArgs src;
    src.term = 8;
    src.candidateId = "node-1";
    src.lastLogIndex = 10;
    src.lastLogTerm = 7;

    for (auto _ : state) {
        RequestVoteArgs c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(log_entry_vote_copy);
