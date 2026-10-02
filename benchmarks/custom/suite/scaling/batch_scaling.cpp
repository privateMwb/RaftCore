// Batch Scaling Benchmark Suite
// Measures how a follower's handleAppendEntries() cost grows with the
// number of entries in one RPC (1, 10, 100 and 1000; empty payloads).
//
// handleAppendEntries() takes its args by value, so every call is handed
// a copy of a prebuilt args object -- as a Transport would hand over a
// decoded message. That copy is part of each timed call; the "copy N"
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
static void bench_copy_1() {
    const AppendEntriesArgs src = makeArgs(1, 7);

    auto copy = [&] {
        AppendEntriesArgs c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("copy 1", copy);
}

// Baseline: copying args that carry 10 entries.
static void bench_copy_10() {
    const AppendEntriesArgs src = makeArgs(10, 7);

    auto copy = [&] {
        AppendEntriesArgs c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("copy 10", copy);
}

// Baseline: copying args that carry 100 entries.
static void bench_copy_100() {
    const AppendEntriesArgs src = makeArgs(100, 7);

    auto copy = [&] {
        AppendEntriesArgs c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("copy 100", copy);
}

// Baseline: copying args that carry 1000 entries.
static void bench_copy_1000() {
    const AppendEntriesArgs src = makeArgs(1000, 7);

    auto copy = [&] {
        AppendEntriesArgs c(src);
        doNotOptimize(c);
    };
    BENCH_SOLO("copy 1000", copy);
}

// Measures a retried RPC of 1 entries, all already present and matching.
static void bench_match_1() {
    Rig rig(1);
    const AppendEntriesArgs src = makeArgs(1, 7);
    AppendEntriesReply reply;

    auto match = [&] {
        Status s = rig.raft.handleAppendEntries(src, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("match 1", match);
}

// Measures a retried RPC of 10 entries, all already present and matching.
static void bench_match_10() {
    Rig rig(10);
    const AppendEntriesArgs src = makeArgs(10, 7);
    AppendEntriesReply reply;

    auto match = [&] {
        Status s = rig.raft.handleAppendEntries(src, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("match 10", match);
}

// Measures a retried RPC of 100 entries, all already present and matching.
static void bench_match_100() {
    Rig rig(100);
    const AppendEntriesArgs src = makeArgs(100, 7);
    AppendEntriesReply reply;

    auto match = [&] {
        Status s = rig.raft.handleAppendEntries(src, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("match 100", match);
}

// Measures a retried RPC of 1000 entries, all already present and matching.
static void bench_match_1000() {
    Rig rig(1000);
    const AppendEntriesArgs src = makeArgs(1000, 7);
    AppendEntriesReply reply;

    auto match = [&] {
        Status s = rig.raft.handleAppendEntries(src, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("match 1000", match);
}

// Measures replacing a 1-entry log wholesale: truncate, then append 1.
static void bench_replace_1() {
    Rig rig(1);
    const AppendEntriesArgs a = makeArgs(1, 8);
    const AppendEntriesArgs b = makeArgs(1, 9);
    AppendEntriesReply reply;
    bool flip = false;

    auto replace = [&] {
        flip = !flip;
        Status s = rig.raft.handleAppendEntries(flip ? a : b, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("replace 1", replace);
}

// Measures replacing a 10-entry log wholesale: truncate, then append 10.
static void bench_replace_10() {
    Rig rig(10);
    const AppendEntriesArgs a = makeArgs(10, 8);
    const AppendEntriesArgs b = makeArgs(10, 9);
    AppendEntriesReply reply;
    bool flip = false;

    auto replace = [&] {
        flip = !flip;
        Status s = rig.raft.handleAppendEntries(flip ? a : b, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("replace 10", replace);
}

// Measures replacing a 100-entry log wholesale: truncate, then append 100.
static void bench_replace_100() {
    Rig rig(100);
    const AppendEntriesArgs a = makeArgs(100, 8);
    const AppendEntriesArgs b = makeArgs(100, 9);
    AppendEntriesReply reply;
    bool flip = false;

    auto replace = [&] {
        flip = !flip;
        Status s = rig.raft.handleAppendEntries(flip ? a : b, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("replace 100", replace);
}

// Measures replacing a 1000-entry log wholesale: truncate, then append 1000.
static void bench_replace_1000() {
    Rig rig(1000);
    const AppendEntriesArgs a = makeArgs(1000, 8);
    const AppendEntriesArgs b = makeArgs(1000, 9);
    AppendEntriesReply reply;
    bool flip = false;

    auto replace = [&] {
        flip = !flip;
        Status s = rig.raft.handleAppendEntries(flip ? a : b, reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("replace 1000", replace);
}

// Executes all batch-scaling benchmark cases.
static void run_benchmarks() {
    bench_copy_1();
    bench_copy_10();
    bench_copy_100();
    bench_copy_1000();
    bench_match_1();
    bench_match_10();
    bench_match_100();
    bench_match_1000();
    bench_replace_1();
    bench_replace_10();
    bench_replace_100();
    bench_replace_1000();
}

REGISTER_BENCH_SUITE();
