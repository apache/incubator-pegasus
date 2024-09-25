/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "common/replication_other_types.h"
#include "log_file.h"
#include "mutation.h"
#include "replica/replica_base.h"
#include "runtime/api_task.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/zlocks.h"

namespace dsn {
class binary_writer;

namespace replication {

class learn_state;
class log_appender;
//
// manage a sequence of continuous mutation log files
// each log file name is: log.{index}.{global_start_offset}
//
// this class is thread safe
//
class replica;

class mutation_log : public ref_counter
{
public:
    // DEPRECATED: The returned bool value will never be evaluated.
    // Always return true in the callback.
    typedef std::function<bool(int log_length, mutation_ptr &)> replay_callback;

    typedef std::function<void(dsn::error_code err)> io_failure_callback;

public:
    // Append a log mutation.
    // Return value: nullptr for error.
    //
    // Thread safe.
    virtual ::dsn::task_ptr append(mutation_ptr &mu,
                                   dsn::task_code callback_code,
                                   dsn::task_tracker *tracker,
                                   aio_handler &&callback,
                                   int hash = 0,
                                   int64_t *pending_size = nullptr) = 0;

    // Get learn state in memory, including pending and writing mutations:
    // - return true if some data is filled into writer
    // - return false if no data is filled into writer
    //
    // Thread safe
    virtual bool get_learn_state_in_memory(decree start_decree, binary_writer &writer) const
    {
        return false;
    }

    // Only for private log.
    // get in-memory mutations, including pending and writing mutations.
    virtual void get_in_memory_mutations(decree start_decree,
                                         ballot current_ballot,
                                         /*out*/ std::vector<mutation_ptr> &mutations_list) const
    {
    }

    // Flush the pending buffer until all data is on disk.
    //
    // Thread safe.
    virtual void flush() = 0;

    // Flush the pending buffer at most once.
    //
    // Thread safe.
    virtual void flush_once() = 0;

public:
    //
    // Ctors
    // when is_private = true, should specify "private_gpid"
    //
    mutation_log(const std::string &dir, int32_t max_log_file_mb, gpid gpid, replica *r = nullptr);

    virtual ~mutation_log() = default;

    //
    // Initialization
    //

    // Open and replay.
    // return ERR_OK if succeed.
    // Not thread safe, but only be called when init.
    error_code open(replay_callback read_callback, io_failure_callback write_error_callback);
    error_code open(replay_callback read_callback,
                    io_failure_callback write_error_callback,
                    const std::map<gpid, decree> &replay_condition);

    // Close the log.
    //
    // Thread safe.
    void close();

    //
    // Replay.
    //
    static error_code replay(std::vector<std::string> &log_files,
                             replay_callback callback,
                             /*out*/ int64_t &end_offset);

    // Reads a series of mutations from the log file (from `start_offset` of `log`),
    // and iterates over the mutations, executing the provided `callback` for each
    // mutation entry.
    // Since the logs are packed into multiple blocks, this function retrieves
    // only one log block at a time.
    //
    // Parameters:
    // - callback: the callback to execute for each mutation.
    // - start_offset: file offset to start.
    //
    // Returns:
    // - ERR_INVALID_DATA: if the loaded data is incorrect or invalid.
    //
    static error_s replay_block(log_file_ptr &log,
                                replay_callback &callback,
                                size_t start_offset,
                                /*out*/ int64_t &end_offset);
    static error_s replay_block(log_file_ptr &log,
                                replay_callback &&callback,
                                size_t start_offset,
                                /*out*/ int64_t &end_offset)
    {
        return replay_block(log, callback, start_offset, end_offset);
    }

    // Resets mutation log with log files under `dir`.
    // The original log will be removed after this call.
    // NOTE: log should be opened before this method called. now it only be used private log
    error_code reset_from(const std::string &dir, replay_callback, io_failure_callback);

    //
    // Maintain max_decree & valid_start_offset
    //

    // valid_start_offset is needed to be set while opening an existing replica.
    //
    // Thread safe.
    void set_valid_start_offset_on_open(gpid gpid, int64_t valid_start_offset);

    // Current max decree is needed to be reset, while creating a new replica.
    // Return current global end offset, should be remebered by caller for gc usage.
    //
    // Thread safe.
    int64_t on_partition_reset(gpid gpid, decree max_decree);

    // Update current max decree.
    //
    // Thread safe.
    void update_max_decree(gpid gpid, decree d);

    // Update current max decree and committed decree that have ever been written onto disk
    // for plog.
    //
    // Thread safe.
    void update_max_decree_on_disk(decree max_decree, decree max_commit);

    //
    //  Garbage collection logs that are already covered by
    //  durable state on disk, return deleted log segment count
    //

    // Garbage collection for private log, returns removed file count.
    //
    // Log files could be removed once all the following conditions are satisfied:
    //  - the file is not the current log file
    //  - the file is not covered by reserve_max_size or reserve_max_time
    //  - file.max_decree <= "durable_decree" || file.end_offset <= "valid_start_offset"
    // which means, files should be reserved if one of the conditions is satisfied:
    //  - the file is the current log file
    //  - the file is covered by both reserve_max_size and reserve_max_time
    //  - file.max_decree > "durable_decree" && file.end_offset > "valid_start_offset"
    //
    // Thread safe.
    int garbage_collection(gpid gpid,
                           decree durable_decree,
                           int64_t valid_start_offset,
                           int64_t reserve_max_size,
                           int64_t reserve_max_time);

    // When this is a private log, log files are learned by remote replicas
    // return true if private log surely covers the learning range.
    bool get_learn_state(gpid gpid, decree start, /*out*/ learn_state &state) const;

    // Only valid for private log.
    //
    // Get parent mutations in memory and private log files during partition split.
    // `total_file_size` is used for the metrics of partition split.
    void get_parent_mutations_and_logs(gpid pid,
                                       decree start_decree,
                                       ballot start_ballot,
                                       /*out*/ std::vector<mutation_ptr> &mutation_list,
                                       /*out*/ std::vector<std::string> &files,
                                       /*out*/ uint64_t &total_file_size) const;

    //
    //  Other inquiry routines
    //

    // Get log dir.
    //
    // Thread safe (because nerver changed).
    const std::string &dir() const { return _dir; }

    // Get current max decree for gpid.
    // Return 0 if not found.
    //
    // Thread safe.
    decree max_decree(gpid gpid) const;

    // Get current max decree on disk for plog.
    //
    // Thread safe.
    decree max_decree_on_disk() const;

    // Get current max committed decree on disk for plog.
    //
    // Thread safe.
    decree max_commit_on_disk() const;

    // Decree of the maximum garbage-collected mutation.
    // For example, given mutations [20, 100], if [20, 50] is garbage-collected,
    // the max_gced_decree=50.
    // Under the real-world cases, the mutations may not be ordered with the file-id.
    // Given 3 log files:
    //   #1:[20, 30], #2:[30, 50], #3:[10, 50]
    // The third file is learned from primary of new epoch. Since it contains mutations smaller
    // than the others, the max_gced_decree = 9.
    // Returns `invalid_decree` when plog directory is empty.
    //
    // Thread safe & private log only.
    decree max_gced_decree(gpid gpid) const;
    decree max_gced_decree_no_lock(gpid gpid) const;

    using log_file_map_by_index = std::map<int, log_file_ptr>;

    // thread-safe
    log_file_map_by_index get_log_file_map() const;

    // Check the consistence of valid_start_offset
    //
    // Thread safe.
    void check_valid_start_offset(gpid gpid, int64_t valid_start_offset) const;

    // Get the total size.
    //
    // Thread safe.
    int64_t total_size() const;

    void hint_switch_file() { _switch_file_hint = true; }
    void demand_switch_file() { _switch_file_demand = true; }

    task_tracker *tracker() { return &_tracker; }

protected:
    // 'size' is data size to write; the '_global_end_offset' will be updated by 'size'.
    // can switch file only when create_new_log_if_needed = true;
    // return pair: the first is target file to write; the second is the global offset to start
    // write.
    //
    // Thread safe.
    std::pair<log_file_ptr, int64_t> mark_new_offset(size_t size, bool create_new_log_if_needed);

    // Thread safe.
    int64_t get_global_offset() const
    {
        zauto_lock l(_lock);
        return _global_end_offset;
    }

    // Init memory states.
    virtual void init_states();

private:
    //
    //  internal helpers
    //
    static error_code replay(log_file_ptr log,
                             replay_callback callback,
                             /*out*/ int64_t &end_offset);

    static error_code replay(log_file_map_by_index &log_files,
                             replay_callback callback,
                             /*out*/ int64_t &end_offset);

    // Update max decree without lock.
    void update_max_decree_no_lock(gpid gpid, decree d);

    // Update max decree on disk without lock.
    void update_max_decree_on_disk_no_lock(decree d);

    // Update max committed decree on disk without lock.
    void update_max_commit_on_disk_no_lock(decree d);

    // create new log file and set it as the current log file
    // returns ERR_OK if create succeed
    // Preconditions:
    // - _pending_write == nullptr (because we need create new pending buffer to write file header)
    // - _lock.locked()
    error_code create_new_log_file();

    // Get total size without lock.
    int64_t total_size_no_lock() const;

protected:
    std::string _dir;
    // TODO(yingchun): Check whether they are useful anymore.
    bool _is_private;
    gpid _private_gpid;      // only used for private log
    replica *_owner_replica; // only used for private log
    io_failure_callback _io_error_callback;

    // options
    int64_t _max_log_file_size_in_bytes;
    int64_t _min_log_file_size_in_bytes;
    bool _force_flush;

    dsn::task_tracker _tracker;

private:
    friend class mutation_log_test;
    friend class mock_mutation_log_private;

    ///////////////////////////////////////////////
    //// memory states
    ///////////////////////////////////////////////
    mutable zlock _lock;
    bool _is_opened;
    bool _switch_file_hint;
    bool _switch_file_demand;

    // logs
    int _last_file_index;             // new log file index = _last_file_index + 1
    log_file_map_by_index _log_files; // index -> log_file_ptr
    log_file_ptr _current_log_file;   // current log file
    int64_t _global_start_offset;     // global start offset of all files.
                                      // invalid if _log_files.size() == 0.
    int64_t _global_end_offset;       // global end offset currently

    // replica log info
    // - log_info.max_decree: the max decree of mutations up to now
    // - log_info.valid_start_offset: the same with replica_init_info::init_offset

    // replica log info for private log
    replica_log_info _private_log_info;

    // The max decree of the mutations that have ever been written onto the disk for plog.
    decree _plog_max_decree_on_disk;

    // The max decree of the committed mutations that have ever been written onto the disk
    // for plog. Since it is set with mutation.data.header.last_committed_decree, it must
    // be less than _plog_max_decree_on_disk.
    decree _plog_max_commit_on_disk;
};

typedef dsn::ref_ptr<mutation_log> mutation_log_ptr;

class mutation_log_private : public mutation_log, private replica_base
{
public:
    // Parameters:
    //  - batch_buffer_max_count, batch_buffer_bytes
    //    The hint of limited size for the write buffer storing the pending mutations.
    //    Note that the actual log block is still possible to be larger than the
    //    hinted size.
    mutation_log_private(const std::string &dir, int32_t max_log_file_mb, gpid gpid, replica *r);

    ~mutation_log_private() override
    {
        close();
        _tracker.cancel_outstanding_tasks();
    }

    ::dsn::task_ptr append(mutation_ptr &mu,
                           dsn::task_code callback_code,
                           dsn::task_tracker *tracker,
                           aio_handler &&callback,
                           int hash = 0,
                           int64_t *pending_size = nullptr) override;

    bool get_learn_state_in_memory(decree start_decree, binary_writer &writer) const override;

    // get in-memory mutations, including pending and writing mutations
    void get_in_memory_mutations(decree start_decree,
                                 ballot start_ballot,
                                 /*out*/ std::vector<mutation_ptr> &mutation_list) const override;

    void flush() override;
    void flush_once() override;

private:
    // async write pending mutations into log file
    // Preconditions:
    // - _pending_write != nullptr
    // - _issued_write.expired() == true (because only one async write is allowed at the same time)
    // release_lock_required should always be true => this function must release the lock
    // appropriately for less lock contention
    void write_pending_mutations(bool release_lock_required);

    void commit_pending_mutations(log_file_ptr &lf,
                                  std::shared_ptr<log_appender> &pending,
                                  decree max_decree,
                                  decree max_commit);

    void init_states() override;

    // flush at most count times
    // if count <= 0, means flush until all data is on disk
    void flush_internal(int max_count);

private:
    // bufferring - only one concurrent write is allowed
    typedef std::vector<mutation_ptr> mutations;
    std::atomic_bool _is_writing;
    // Writes that are emitted to `commit_log_block` but are not completely written.
    // The weak_ptr used here is a trick. Once the pointer freed, ie.
    // `_issued_write.lock() == nullptr`, it means the emitted writes all finished.
    std::weak_ptr<log_appender> _issued_write;
    std::shared_ptr<log_appender> _pending_write;
    decree _pending_write_max_commit;
    decree _pending_write_max_decree;
    mutable zlock _plock;
};

} // namespace replication
} // namespace dsn
