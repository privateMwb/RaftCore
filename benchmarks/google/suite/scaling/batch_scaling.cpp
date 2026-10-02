// Batch Scaling Benchmark Suite
// Measures how a follower's handleAppendEntries() cost grows with the
// number of entries in one RPC (1, 10, 100 and 1000; empty payloads).
//
// handleAppendEntries() takes its args by value, so every call is handed
// a copy of a prebuilt args object -- as a Transport would hand over a
// decoded message. That copy is part of each timed call; the batch_scaling_copy_N
// cases time it alone so it can be subtracted.
//
// Covers, at each batch size:
// - copy baseline: copying the args, nothing else
// - match: every entry is already in the log with a matching term (a
//   retried RPC) -- N log reads, no writes
// - replace: every entry conflicts with what's stored, so the follower
//   truncates the whole log and appends N entries. Two prebuilt args
//   with different entry terms alternate, so each call conflicts with
//   the previous one and the log stays at N entries

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Builds AppendEntries args at term 7 from prevLogIndex 0 (no prev-entry
// check), carrying `entryCount` empty-payload entries of term `entryTerm`.
static AppendEntriesArgs makeArgs(std::size_t entryCount, Term entryTerm) {
    AppendEntriesArgs args;
    args.term = 7;
    args.leaderId = "node-2";
    for (std::size_t i = 0; i < entryCount; ++i) {
        LogEntry entry;
        entry.term = entryTerm;
        args.entries.push_back(std::move(entry));
    }
    return args;
}

// Baseline: copying args that carry 1 entries.
static void batch_scaling_copy_1(benchmark::State& state) {
    const AppendEntriesArgs src = makeArgs(1, 7);

    for (auto _ : state) {
        AppendEntriesArgs c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(batch_scaling_copy_1);

// Baseline: copying args that carry 10 entries.
static void batch_scaling_copy_10(benchmark::State& state) {
    const AppendEntriesArgs src = makeArgs(10, 7);

    for (auto _ : state) {
        AppendEntriesArgs c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(batch_scaling_copy_10);

// Baseline: copying args that carry 100 entries.
static void batch_scaling_copy_100(benchmark::State& state) {
    const AppendEntriesArgs src = makeArgs(100, 7);

    for (auto _ : state) {
        AppendEntriesArgs c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(batch_scaling_copy_100);

// Baseline: copying args that carry 1000 entries.
static void batch_scaling_copy_1000(benchmark::State& state) {
    const AppendEntriesArgs src = makeArgs(1000, 7);

    for (auto _ : state) {
        AppendEntriesArgs c(src);
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(batch_scaling_copy_1000);

// Measures a retried RPC of 1 entries, all already present and matching.
static void batch_scaling_match_1(benchmark::State& state) {
    Rig rig(1);
    const AppendEntriesArgs src = makeArgs(1, 7);
    AppendEntriesReply reply;

    for (auto _ : state) {
        Status s = rig.raft.handleAppendEntries(src, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(batch_scaling_match_1);

// Measures a retried RPC of 10 entries, all already present and matching.
static void batch_scaling_match_10(benchmark::State& state) {
    Rig rig(10);
    const AppendEntriesArgs src = makeArgs(10, 7);
    AppendEntriesReply reply;

    for (auto _ : state) {
        Status s = rig.raft.handleAppendEntries(src, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(batch_scaling_match_10);

// Measures a retried RPC of 100 entries, all already present and matching.
static void batch_scaling_match_100(benchmark::State& state) {
    Rig rig(100);
    const AppendEntriesArgs src = makeArgs(100, 7);
    AppendEntriesReply reply;

    for (auto _ : state) {
        Status s = rig.raft.handleAppendEntries(src, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(batch_scaling_match_100);

// Measures a retried RPC of 1000 entries, all already present and matching.
static void batch_scaling_match_1000(benchmark::State& state) {
    Rig rig(1000);
    const AppendEntriesArgs src = makeArgs(1000, 7);
    AppendEntriesReply reply;

    for (auto _ : state) {
        Status s = rig.raft.handleAppendEntries(src, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(batch_scaling_match_1000);

// Measures replacing a 1-entry log wholesale: truncate, then append 1.
static void batch_scaling_replace_1(benchmark::State& state) {
    Rig rig(1);
    const AppendEntriesArgs a = makeArgs(1, 8);
    const AppendEntriesArgs b = makeArgs(1, 9);
    AppendEntriesReply reply;
    bool flip = false;

    for (auto _ : state) {
        flip = !flip;
        Status s = rig.raft.handleAppendEntries(flip ? a : b, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(batch_scaling_replace_1);

// Measures replacing a 10-entry log wholesale: truncate, then append 10.
static void batch_scaling_replace_10(benchmark::State& state) {
    Rig rig(10);
    const AppendEntriesArgs a = makeArgs(10, 8);
    const AppendEntriesArgs b = makeArgs(10, 9);
    AppendEntriesReply reply;
    bool flip = false;

    for (auto _ : state) {
        flip = !flip;
        Status s = rig.raft.handleAppendEntries(flip ? a : b, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(batch_scaling_replace_10);

// Measures replacing a 100-entry log wholesale: truncate, then append 100.
static void batch_scaling_replace_100(benchmark::State& state) {
    Rig rig(100);
    const AppendEntriesArgs a = makeArgs(100, 8);
    const AppendEntriesArgs b = makeArgs(100, 9);
    AppendEntriesReply reply;
    bool flip = false;

    for (auto _ : state) {
        flip = !flip;
        Status s = rig.raft.handleAppendEntries(flip ? a : b, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(batch_scaling_replace_100);

// Measures replacing a 1000-entry log wholesale: truncate, then append 1000.
static void batch_scaling_replace_1000(benchmark::State& state) {
    Rig rig(1000);
    const AppendEntriesArgs a = makeArgs(1000, 8);
    const AppendEntriesArgs b = makeArgs(1000, 9);
    AppendEntriesReply reply;
    bool flip = false;

    for (auto _ : state) {
        flip = !flip;
        Status s = rig.raft.handleAppendEntries(flip ? a : b, reply);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(reply);
    }
}
BENCHMARK(batch_scaling_replace_1000);
