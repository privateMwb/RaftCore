/**
 * @file            RandomSource.h
 *
 * @date            2026-9-29
 *
 * @version         1.0.0
 *
 * @copyright       Copyright (c) 2026 privateMWB
 *                  All rights reserved.
 *
 * @attention       This source is released under the MIT license
 *                  SPDX-License-Identifier: MIT
 *                  <http://opensource.org/licenses/MIT>
 */

#pragma once

// clang-format off
#include <random> // SystemRandomSource's mt19937/random_device
// clang-format on

// Injectable source of randomness for election-timeout jitter. Added in
// Phase 2, alongside Storage/PersistentState/Transport/StateMachine:
// the roadmap's open question about a simulated/injectable clock is
// answered by RaftCore being tick()-driven with no timers of its own
// (see RaftNode.h), but the randomized timeout *value* itself still
// needs a source -- and that source must be swappable for a scripted
// one in tests, or split-vote/election tests (Phase 7) couldn't force
// a specific sequence of timeouts deterministically.

namespace RaftCore {

/// @brief Produces integers, e.g. for picking a randomized election timeout.
class RandomSource {
  public:
    virtual ~RandomSource() = default;

    /// @brief Returns a value in [minInclusive, maxInclusive].
    /// @attention `minInclusive <= maxInclusive` is a precondition; the
    /// implementation is not required to validate it.
    virtual int nextInt(int minInclusive, int maxInclusive) = 0;
};

/**
 * @brief A real RandomSource backed by std::mt19937, seeded from std::random_device.
 * @attention Provided directly by RaftCore, unlike Storage/Transport --
 * there is no existing ecosystem library (WriteAheadLog, FalconHTTP) this
 * wraps, and no reason to make every consumer write their own trivial
 * PRNG wrapper. Tests should still prefer a scripted RandomSource over
 * this one, for reproducibility. Not thread-safe: fine under RaftCore's
 * single-threaded tick()/receive() model (see RaftNode.h), not fine if
 * shared across multiple RaftNode instances driven from different threads.
 */
class SystemRandomSource : public RandomSource {
  public:
    SystemRandomSource() : engine_(std::random_device{}()) {}

    int nextInt(int minInclusive, int maxInclusive) override {
        std::uniform_int_distribution<int> distribution(minInclusive, maxInclusive);
        return distribution(engine_);
    }

  private:
    std::mt19937 engine_;
};

} // namespace RaftCore
