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

#include <benchmark/benchmark.h>

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
static void random_source_sys_ctor(benchmark::State& state) {

    for (auto _ : state) {
        SystemRandomSource r;
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(random_source_sys_ctor);

// Measures nextInt() over a typical election-timeout range.
static void random_source_sys_narrow(benchmark::State& state) {
    SystemRandomSource sys;
    RandomSource* r = &sys;
    benchmark::DoNotOptimize(r);

    for (auto _ : state) {
        int v = r->nextInt(150, 300);
        benchmark::DoNotOptimize(v);
    }
}
BENCHMARK(random_source_sys_narrow);

// Measures nextInt() over a wide range.
static void random_source_sys_wide(benchmark::State& state) {
    SystemRandomSource sys;
    RandomSource* r = &sys;
    benchmark::DoNotOptimize(r);

    for (auto _ : state) {
        int v = r->nextInt(0, 1000000);
        benchmark::DoNotOptimize(v);
    }
}
BENCHMARK(random_source_sys_wide);

// Measures FakeRandom::nextInt() -- the floor: virtual call, no work.
static void random_source_fake(benchmark::State& state) {
    FakeRandom fake;
    RandomSource* r = &fake;
    benchmark::DoNotOptimize(r);

    for (auto _ : state) {
        int v = r->nextInt(150, 300);
        benchmark::DoNotOptimize(v);
    }
}
BENCHMARK(random_source_fake);

// Measures a scripted source cycling through 5 values.
static void random_source_scripted(benchmark::State& state) {
    ScriptedRandom scripted;
    for (int v : {150, 200, 250, 300, 175})
        scripted.add(v);
    RandomSource* r = &scripted;
    benchmark::DoNotOptimize(r);

    for (auto _ : state) {
        int v = r->nextInt(150, 300);
        benchmark::DoNotOptimize(v);
    }
}
BENCHMARK(random_source_scripted);
