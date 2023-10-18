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

#include <fmt/core.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <iosfwd>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "common/replication_other_types.h"
#include "log_file.h"
#include "mutation.h"
#include "replica/replica_base.h"
#include "runtime/api_task.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fmt_utils.h"
#include "utils/zlocks.h"

namespace dsn {
class binary_writer;
class perf_counter_wrapper;

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
    // append a log mutation
    // return value: nullptr for error
    // thread safe
    virtual ::dsn::task_ptr append(mutation_ptr &mu,
                                   dsn::task_code callback_code,
                                   dsn::task_tracker *tracker,
                                   aio_handler &&callback,
                                   int hash = 0,
                                   int64_t *pending_size = nullptr) = 0;

    // get learn state in memory, including pending and writing mutations
    // return true if some data is filled into writer
    // return false if no data is filled into writer
    // thread safe
    virtual bool get_learn_state_in_memory(decree start_decree, binary_writer &writer) const
    {
        return false;
    }

    // only for private log
    // get in-memory mutations, including pending and writing mutations
    virtual void get_in_memory_mutations(decree start_decree,
                                         ballot current_ballot,
                                         /*out*/ std::vector<mutation_ptr> &mutations_list) const
    {
    }

    // flush the pending buffer until all data is on disk
    // thread safe
    virtual void flush() = 0;

    // flush the pending buffer at most once
    // thread safe
    virtual void flush_once() = 0;

public:
    //
    // ctors
    // when is_private = true, should specify "private_gpid"
    //
    mutation_log(const std::string &dir, int32_t max_log_file_mb, gpid gpid, replica *r = nullptr);

    virtual ~mutation_log() = default;

    //
    // initialization
    //

    // open and replay
    // returns ERR_OK if succeed
    // not thread safe, but only be called when init
    error_code open(replay_callback read_callback, io_failure_callback write_error_callback);
    error_code open(replay_callback read_callback,
                    io_failure_callback write_error_callback,
                    const std::map<gpid, decree> &replay_condition);
    // close the log
    // thread safe
    void close();

    //
    // replay
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
    // maintain max_decree & valid_start_offset
    //

    // when open a exist replica, need to set valid_start_offset on open
    // thread safe
    void set_valid_start_offset_on_open(gpid gpid, int64_t valid_start_offset);

    // when create a new replica, need to reset current max decree
    // returns current global end offset, needs to be remebered by caller for gc usage
    // thread safe
    int64_t on_partition_reset(gpid gpid, decree max_decree);

    // remove entry from _previous_log_max_decrees when a partition is removed.
    // only used for private log.
    // thread safe
    void on_partition_removed(gpid gpid);

    // update current max decree
    // thread safe
    void update_max_decree(gpid gpid, decree d);

    // update current max commit of private log
    // thread safe
    void update_max_commit_on_disk(decree d);

    //
    //  garbage collection logs that are already covered by
    //  durable state on disk, return deleted log segment count
    //

    // garbage collection for private log, returns removed file count.
    // can remove log files if satisfy all the conditions:
    //  - the file is not the current log file
    //  - the file is not covered by reserve_max_size or reserve_max_time
    //  - file.max_decree <= "durable_decree" || file.end_offset <= "valid_start_offset"
    // that means, should reserve files if satisfy one of the conditions:
    //  - the file is the current log file
    //  - the file is covered by both reserve_max_size and reserve_max_time
    //  - file.max_decree > "durable_decree" && file.end_offset > "valid_start_offset"
    // thread safe
    int garbage_collection(gpid gpid,
                           decree durable_decree,
                           int64_t valid_start_offset,
                           int64_t reserve_max_size,
                           int64_t reserve_max_time);

    // Garbage collection for shared log.
    // `prevent_gc_replicas' will store replicas which prevent log files from being deleted
    // for gc.
    //
    // Since slog had been deprecated, no new slog files would be created. Therefore, our
    // target is to remove all of the existing slog files according to the progressive durable
    // decree for each replica.
    //
    // Thread safe.
    void garbage_collection(const replica_log_info_map &replica_durable_decrees,
                            std::set<gpid> &prevent_gc_replicas);

    //
    // when this is a private log, log files are learned by remote replicas
    // return true if private log surely covers the learning range
    //
    bool get_learn_state(gpid gpid, decree start, /*out*/ learn_state &state) const;

    // only valid for private log
    // get parent mutations in memory and private log files during partition split
    // total_file_size is used for split perf-counter
    void get_parent_mutations_and_logs(gpid pid,
                                       decree start_decree,
                                       ballot start_ballot,
                                       /*out*/ std::vector<mutation_ptr> &mutation_list,
                                       /*out*/ std::vector<std::string> &files,
                                       /*out*/ uint64_t &total_file_size) const;

    //
    //  other inquiry routines
    //

    // log dir
    // thread safe (because nerver changed)
    const std::string &dir() const { return _dir; }

    // replica
    replica *owner_replica() const { return _owner_replica; }

    // get current max decree for gpid
    // returns 0 if not found
    // thread safe
    decree max_decree(gpid gpid) const;

    // get current max commit on disk of private log.
    // thread safe
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
    // thread-safe & private log only
    decree max_gced_decree(gpid gpid) const;
    decree max_gced_decree_no_lock(gpid gpid) const;

    using log_file_map_by_index = std::map<int, log_file_ptr>;

    // thread-safe
    log_file_map_by_index get_log_file_map() const;

    // check the consistence of valid_start_offset
    // thread safe
    void check_valid_start_offset(gpid gpid, int64_t valid_start_offset) const;

    // get total size.
    int64_t total_size() const;

    void hint_switch_file() { _switch_file_hint = true; }
    void demand_switch_file() { _switch_file_demand = true; }

    task_tracker *tracker() { return &_tracker; }

    struct reserved_slog_info
    {
        size_t file_count = 0;
        int64_t log_size = 0;
        int min_file_index = 0;
        int max_file_index = 0;

        std::string to_string() const
        {
            return fmt::format("reserved_slog_info = [file_count = {}, log_size = {}, "
                               "min_file_index = {}, max_file_index = {}]",
                               file_count,
                               log_size,
                               min_file_index,
                               max_file_index);
        }

        friend std::ostream &operator<<(std::ostream &os, const reserved_slog_info &reserved_log)
        {
            return os << reserved_log.to_string();
        }
    };

    struct slog_deletion_info
    {
        int to_delete_file_count = 0;
        int64_t to_delete_log_size = 0;
        int deleted_file_count = 0;
        int64_t deleted_log_size = 0;
        int deleted_min_file_index = 0;
        int deleted_max_file_index = 0;

        std::string to_string() const
        {
            return fmt::format("slog_deletion_info = [to_delete_file_count = {}, "
                               "to_delete_log_size = {}, deleted_file_count = {}, "
                               "deleted_log_size = {}, deleted_min_file_index = {}, "
                               "deleted_max_file_index = {}]",
                               to_delete_file_count,
                               to_delete_log_size,
                               deleted_file_count,
                               deleted_log_size,
                               deleted_min_file_index,
                               deleted_max_file_index);
        }

        friend std::ostream &operator<<(std::ostream &os, const slog_deletion_info &log_deletion)
        {
            return os << log_deletion.to_string();
        }
    };

protected:
    // thread-safe
    // 'size' is data size to write; the '_global_end_offset' will be updated by 'size'.
    // can switch file only when create_new_log_if_needed = true;
    // return pair: the first is target file to write; the second is the global offset to start
    // write
    std::pair<log_file_ptr, int64_t> mark_new_offset(size_t size, bool create_new_log_if_needed);
    // thread-safe
    int64_t get_global_offset() const
    {
        zauto_lock l(_lock);
        return _global_end_offset;
    }

    // init memory states
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

    // update max decree without lock
    void update_max_decree_no_lock(gpid gpid, decree d);

    // update max commit on disk without lock
    void update_max_commit_on_disk_no_lock(decree d);

    // create new log file and set it as the current log file
    // returns ERR_OK if create succeed
    // Preconditions:
    // - _pending_write == nullptr (because we need create new pending buffer to write file header)
    // - _lock.locked()
    error_code create_new_log_file();

    // get total size ithout lock.
    int64_t total_size_no_lock() const;

    // Closing and remove all of slog files whose indexes are less than (i.e. older) or equal to
    // `max_file_index_to_delete`.
    void remove_obsolete_slog_files(const int max_file_index_to_delete,
                                    log_file_map_by_index &files,
                                    reserved_slog_info &reserved_log,
                                    slog_deletion_info &log_deletion);

protected:
    std::string _dir;
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
    friend class mock_mutation_log_shared;

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

    // replica log info for shared log
    replica_log_info_map _shared_log_info_map;

    // replica log info for private log
    replica_log_info _private_log_info;
    decree
        _private_max_commit_on_disk; // the max last_committed_decree of written mutations up to now
                                     // used for limiting garbage collection of shared log, because
                                     // the ending of private log should be covered by shared log
};
typedef dsn::ref_ptr<mutation_log> mutation_log_ptr;

class mutation_log_shared : public mutation_log
{
public:
    mutation_log_shared(const std::string &dir,
                        int32_t max_log_file_mb,
                        bool force_flush,
                        perf_counter_wrapper *write_size_counter = nullptr)
        : mutation_log(dir, max_log_file_mb, dsn::gpid(), nullptr),
          _is_writing(false),
          _force_flush(force_flush),
          _write_size_counter(write_size_counter)
    {
    }

    ~mutation_log_shared() override
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

    void commit_pending_mutations(log_file_ptr &lf, std::shared_ptr<log_appender> &pending);

    // flush at most count times
    // if count <= 0, means flush until all data is on disk
    void flush_internal(int max_count);

private:
    // bufferring - only one concurrent write is allowed
    mutable zlock _slock;
    std::atomic_bool _is_writing;
    std::shared_ptr<log_appender> _pending_write;

    bool _force_flush;
    perf_counter_wrapper *_write_size_counter;
};

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

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::replication::mutation_log::reserved_slog_info);
USER_DEFINED_STRUCTURE_FORMATTER(::dsn::replication::mutation_log::slog_deletion_info);
