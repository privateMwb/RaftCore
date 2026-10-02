// Random Source Benchmark Suite
// Measures the RandomSource implementations: the real
// SystemRandomSource (mt19937 seeded from random_device) against the
// trivial test doubles. nextInt() runs once per election-timer reset,
// so it's off the per-tick path, but the real source's construction is
// heavy (a random_device read plus seeding a 624-word engine).
//
// Every nextInt case calls through a RandomSource pointer, as RaftNode
// does, so virtual dispatch is part of what's measured.
//
// Covers:
// - SystemRandomSource construction
// - SystemRandomSource::nextInt() over a typical timeout range and a
//   wide range
// - FakeRandom::nextInt() (returns the lower bound)
// - a scripted source cycling through a fixed value list

#include <support/framework.h>

using namespace RaftCore;
using namespace RaftBench;

namespace {

// Returns a fixed list of values in order, repeating, ignoring the
// requested range -- the shape a test uses to force a specific sequence
// of election timeouts.
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

} // namespace

// Measures constructing a SystemRandomSource (random_device read + engine seed).
static void bench_sys_ctor() {
    auto build = [&] {
        SystemRandomSource r;
        doNotOptimize(r);
    };
    BENCH_SOLO("sys ctor", build);
}

// Measures nextInt() over a typical election-timeout range.
static void bench_sys_narrow() {
    SystemRandomSource sys;
    RandomSource* r = &sys;
    doNotOptimize(r);

    auto draw = [&] {
        int v = r->nextInt(150, 300);
        doNotOptimize(v);
    };
    BENCH_SOLO("sys 150-300", draw);
}

// Measures nextInt() over a wide range.
static void bench_sys_wide() {
    SystemRandomSource sys;
    RandomSource* r = &sys;
    doNotOptimize(r);

    auto draw = [&] {
        int v = r->nextInt(0, 1000000);
        doNotOptimize(v);
    };
    BENCH_SOLO("sys 0-1M", draw);
}

// Measures FakeRandom::nextInt() -- the floor: virtual call, no work.
static void bench_fake() {
    FakeRandom fake;
    RandomSource* r = &fake;
    doNotOptimize(r);

    auto draw = [&] {
        int v = r->nextInt(150, 300);
        doNotOptimize(v);
    };
    BENCH_SOLO("fake", draw);
}

// Measures a scripted source cycling through 5 values.
static void bench_scripted() {
    ScriptedRandom scripted;
    for (int v : {150, 200, 250, 300, 175})
        scripted.add(v);
    RandomSource* r = &scripted;
    doNotOptimize(r);

    auto draw = [&] {
        int v = r->nextInt(150, 300);
        doNotOptimize(v);
    };
    BENCH_SOLO("script", draw);
}

// Executes all RandomSource benchmark cases.
static void run_benchmarks() {
    bench_sys_ctor();
    bench_sys_narrow();
    bench_sys_wide();
    bench_fake();
    bench_scripted();
}

REGISTER_BENCH_SUITE();
