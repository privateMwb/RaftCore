/**
 * @file            Storage.h
 *
 * @date            2026-9-29
 *
 * @version         0.1.0
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
#include <RaftCore/Common/Status.h>  // Status
#include <RaftCore/Common/Types.h>   // Term, LogIndex, LogEntry
#include <VectorPro/Vector.h>  // range()'s out-param
// clang-format on

// Log-storage abstraction RaftCore depends on. RaftCore never names
// MiniDB::Storage::WriteAheadLog directly -- the real app binds a
// WriteAheadLog-backed implementation of this interface; tests bind a
// trivial in-memory one. Dependency inversion is the point: this
// interface is what makes RaftCore reusable outside this one app.

namespace RaftCore {

using namespace VectorPro;

/**
 * @brief Durable, ordered log the Raft algorithm reads and mutates.
 * @attention append() must be durable (fsync'd) before returning
 * `Status::OK` -- RaftCore replies to the RPC that triggered the append
 * immediately afterward, trusting it already survived a crash. Whatever
 * the WriteAheadLog fsync-batching question decides, this safety
 * property is non-negotiable.
 */
class Storage {
  public:
    virtual ~Storage() = default;

    /**
     * @brief Appends one entry to the log.
     * @param term Term the entry was created in.
     * @param payload Opaque bytes to store.
     * @param outIndex Set to the entry's 1-based index on success.
     * @return `Status::OK` on success, `Status::IO_ERROR` on any failure.
     */
    [[nodiscard]] virtual Status append(Term term, Vector<std::uint8_t> payload,
                                        LogIndex& outIndex) = 0;

    /**
     * @brief Reads the entry at `index`.
     * @param index 1-based log index to read.
     * @param outEntry Set to the stored entry on success.
     * @return `Status::OK` on success, `Status::NOT_FOUND` if `index` is
     * out of range or the log is empty.
     */
    [[nodiscard]] virtual Status entryAt(LogIndex index, LogEntry& outEntry) const = 0;

    /**
     * @brief Reads entries `[fromIndex, toIndex]`, inclusive both ends.
     * @param fromIndex First index to read.
     * @param toIndex Last index to read.
     * @param outEntries Set to the matching entries, in order, on success.
     * @return `Status::OK` on success, `Status::NOT_FOUND` if the range
     * falls outside the log.
     */
    [[nodiscard]] virtual Status range(LogIndex fromIndex, LogIndex toIndex,
                                       Vector<LogEntry>& outEntries) const = 0;

    /**
     * @brief Discards every entry at and after `index`.
     * @param index First index to discard. No-op if `index > lastIndex()`.
     * @return `Status::OK` on success, `Status::IO_ERROR` on failure.
     */
    [[nodiscard]] virtual Status truncateFrom(LogIndex index) = 0;

    /// `kNoIndex` if the log is empty. Not fallible -- a pure in-memory query.
    virtual LogIndex lastIndex() const = 0;

    /// `kNoTerm` if the log is empty. Not fallible -- a pure in-memory query.
    virtual Term lastTerm() const = 0;
};

} // namespace RaftCore
