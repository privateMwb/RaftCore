// Append Entries Benchmark Suite
// Measures the follower side of AppendEntries: each branch of the
// receiver rules, from cheap rejections up to truncate-and-append.
//
// Args are built inside each timed call because handleAppendEntries()
// takes them by value and consumes the payloads; the build cost (a
// small struct, plus one LogEntry where entries are sent) is part of
// every case here.
//
// Covers:
// - stale term (rejected immediately)
// - heartbeat (no prev entry, no entries)
// - prev-entry check passing, and mismatching
// - already-present entry (retried RPC: nothing re-appended)
// - new entry appended
// - conflicting entry (truncate, then append)

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

// Builds AppendEntries args from leader "node-2" at term 7, with
// `entryCount` entries all of term `entryTerm`.
static AppendEntriesArgs makeArgs(Term term, LogIndex prevIndex, Term prevTerm,
                                  std::size_t entryCount, Term entryTerm) {
    AppendEntriesArgs args;
    args.term = term;
    args.leaderId = "node-2";
    args.prevLogIndex = prevIndex;
    args.prevLogTerm = prevTerm;
    for (std::size_t i = 0; i < entryCount; ++i) {
        LogEntry entry;
        entry.term = entryTerm;
        args.entries.push_back(std::move(entry));
    }
    return args;
}

// Measures handleAppendEntries() from a stale-term leader.
static void bench_ae_stale() {
    Rig rig(10);
    AppendEntriesReply reply;

    auto stale = [&] {
        Status s = rig.raft.handleAppendEntries(makeArgs(1, 0, 0, 0, 0), reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("ae stale", stale);
}

// Measures a bare heartbeat: valid term, no prev entry, no entries --
// timer reset and reply only.
static void bench_ae_heartbeat() {
    Rig rig(10);
    AppendEntriesReply reply;

    auto beat = [&] {
        Status s = rig.raft.handleAppendEntries(makeArgs(7, 0, 0, 0, 0), reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("ae beat", beat);
}

// Measures a heartbeat whose prev-entry check passes -- adds one log read.
static void bench_ae_check() {
    Rig rig(10);
    AppendEntriesReply reply;

    auto check = [&] {
        Status s = rig.raft.handleAppendEntries(makeArgs(7, 10, 7, 0, 0), reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("ae check", check);
}

// Measures a prev-entry term mismatch -- rejected after the log read.
static void bench_ae_mismatch() {
    Rig rig(10);
    AppendEntriesReply reply;

    auto mismatch = [&] {
        Status s = rig.raft.handleAppendEntries(makeArgs(7, 5, 6, 0, 0), reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("ae mismatch", mismatch);
}

// Measures a retried RPC: the one entry sent is already in the log with
// a matching term, so nothing is truncated or appended.
static void bench_ae_dup() {
    Rig rig(11);
    AppendEntriesReply reply;

    auto dup = [&] {
        Status s = rig.raft.handleAppendEntries(makeArgs(7, 10, 7, 1, 7), reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("ae dup", dup);
}

// Measures appending one new entry at the end of the log. Each call
// extends the log, so prevLogIndex tracks lastIndex().
static void bench_ae_append() {
    Rig rig(10);
    AppendEntriesReply reply;

    auto append = [&] {
        Status s = rig.raft.handleAppendEntries(makeArgs(7, rig.st.lastIndex(), 7, 1, 7), reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("ae append", append);
}

// Measures conflict resolution: the entry at index 10 always has a
// different term than what's stored (the term counter increments every
// call), so each call truncates index 10 and appends the replacement.
static void bench_ae_conflict() {
    Rig rig(10);
    AppendEntriesReply reply;
    Term entryTerm = 7;

    auto conflict = [&] {
        Status s = rig.raft.handleAppendEntries(makeArgs(7, 9, 7, 1, ++entryTerm), reply);
        doNotOptimize(s);
        doNotOptimize(reply);
    };
    BENCH_SOLO("ae conflict", conflict);
}

// Executes all AppendEntries receiver benchmark cases.
static void run_benchmarks() {
    bench_ae_stale();
    bench_ae_heartbeat();
    bench_ae_check();
    bench_ae_mismatch();
    bench_ae_dup();
    bench_ae_append();
    bench_ae_conflict();
}

REGISTER_BENCH_SUITE();
