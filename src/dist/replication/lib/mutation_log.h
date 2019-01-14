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

/*
 * Description:
 *     Mutation log read and write.
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     Dec., 2015, @qinzuoyan (Zuoyan Qin), refactor and add comments
 */

#pragma once

#include "dist/replication/common/replication_common.h"
#include "mutation.h"
#include <atomic>
#include <dsn/tool-api/zlocks.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>

namespace dsn {
namespace replication {

class log_file;
typedef dsn::ref_ptr<log_file> log_file_ptr;

// a structure to record replica's log info
struct replica_log_info
{
    int64_t max_decree;
    int64_t valid_start_offset; // valid start offset in global space
    replica_log_info(int64_t d, int64_t o)
    {
        max_decree = d;
        valid_start_offset = o;
    }
    replica_log_info()
    {
        max_decree = 0;
        valid_start_offset = 0;
    }
    bool operator==(const replica_log_info &o) const
    {
        return max_decree == o.max_decree && valid_start_offset == o.valid_start_offset;
    }
};

typedef std::unordered_map<gpid, replica_log_info> replica_log_info_map;

// each block in log file has a log_block_header
struct log_block_header
{
    int32_t magic;    // 0xdeadbeef
    int32_t length;   // block data length (not including log_block_header)
    int32_t body_crc; // block data crc (not including log_block_header)
    uint32_t
        local_offset; // start offset of the block (including log_block_header) in this log file
};

// each log file has a log_file_header stored at the beginning of the first block's data content
struct log_file_header
{
    int32_t magic;   // 0xdeadbeef
    int32_t version; // current 0x1
    int64_t
        start_global_offset; // start offset in the global space, equals to the file name's postfix
};

// a memory structure holding data which belongs to one block.
class log_block /* : public ::dsn::transient_object*/
{
    std::vector<blob> _data; // the first blob is log_block_header
    size_t _size;            // total data size of all blobs
public:
    log_block() : _size(0) {}
    log_block(blob &&init_blob) : _data({init_blob}), _size(init_blob.length()) {}
    // get all blobs in the block
    const std::vector<blob> &data() const { return _data; }
    // get the first blob (which contains the log_block_header) from the block
    blob &front()
    {
        dassert(!_data.empty(), "trying to get first blob out of an empty log block");
        return _data.front();
    }
    // add a blob into the block
    void add(const blob &bb)
    {
        _size += bb.length();
        _data.push_back(bb);
    }
    // return total data size in the block
    size_t size() const { return _size; }
};

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
    // return true when the mutation's offset is not less than
    // the remembered (shared or private) valid_start_offset therefore valid for the replica
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
    virtual ~mutation_log();

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

    // garbage collection for shared log, returns reserved file count.
    // `prevent_gc_replicas' will store replicas which prevent log files out of `file_count_limit'
    // to be deleted.
    // remove log files if satisfy:
    //  - for each replica "r":
    //         r is not in file.max_decree
    //      || file.max_decree[r] <= gc_condition[r].max_decree
    //      || file.end_offset[r] <= gc_condition[r].valid_start_offset
    //  - the current log file should not be removed
    // thread safe
    int garbage_collection(const replica_log_info_map &gc_condition,
                           int file_count_limit,
                           std::set<gpid> &prevent_gc_replicas);

    //
    // when this is a private log, log files are learned by remote replicas
    // return true if private log surely covers the learning range
    //
    bool get_learn_state(gpid gpid, decree start, /*out*/ learn_state &state) const;

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

    // maximum decree that is garbage collected
    // thread safe
    decree max_gced_decree(gpid gpid, int64_t valid_start_offset) const;

    // check the consistence of valid_start_offset
    // thread safe
    void check_valid_start_offset(gpid gpid, int64_t valid_start_offset) const;

    // get total size.
    int64_t total_size() const;

    void hint_switch_file() { _switch_file_hint = true; }
    void demand_switch_file() { _switch_file_demand = true; }

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

    static error_code replay(std::map<int, log_file_ptr> &log_files,
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
    ///////////////////////////////////////////////
    //// memory states
    ///////////////////////////////////////////////
    mutable zlock _lock;
    bool _is_opened;
    bool _switch_file_hint;
    bool _switch_file_demand;

    // logs
    int _last_file_index;                   // new log file index = _last_file_index + 1
    std::map<int, log_file_ptr> _log_files; // index -> log_file_ptr
    log_file_ptr _current_log_file;         // current log file
    int64_t _global_start_offset;           // global start offset of all files.
                                            // invalid if _log_files.size() == 0.
    int64_t _global_end_offset;             // global end offset currently

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
          _pending_write_start_offset(0),
          _force_flush(force_flush),
          _write_size_counter(write_size_counter)
    {
    }

    virtual ~mutation_log_shared() override { _tracker.cancel_outstanding_tasks(); }
    virtual ::dsn::task_ptr append(mutation_ptr &mu,
                                   dsn::task_code callback_code,
                                   dsn::task_tracker *tracker,
                                   aio_handler &&callback,
                                   int hash = 0,
                                   int64_t *pending_size = nullptr) override;

    virtual void flush() override;
    virtual void flush_once() override;

private:
    // async write pending mutations into log file
    // Preconditions:
    // - _pending_write != nullptr
    // - _issued_write.expired() == true (because only one async write is allowed at the same time)
    // release_lock_required should always be true => this function must release the lock
    // appropriately for less lock contention
    void write_pending_mutations(bool release_lock_required);

    // flush at most count times
    // if count <= 0, means flush until all data is on disk
    void flush_internal(int max_count);

private:
    // bufferring - only one concurrent write is allowed
    typedef std::vector<aio_task_ptr> callbacks;
    typedef std::vector<mutation_ptr> mutations;
    mutable zlock _slock;
    std::atomic_bool _is_writing;
    std::shared_ptr<log_block> _pending_write;
    std::shared_ptr<callbacks> _pending_write_callbacks;
    std::shared_ptr<mutations> _pending_write_mutations;
    int64_t _pending_write_start_offset;

    bool _force_flush;
    perf_counter_wrapper *_write_size_counter;
};

class mutation_log_private : public mutation_log
{
public:
    mutation_log_private(const std::string &dir,
                         int32_t max_log_file_mb,
                         gpid gpid,
                         replica *r,
                         uint32_t batch_buffer_bytes,
                         uint32_t batch_buffer_max_count,
                         uint64_t batch_buffer_flush_interval_ms)
        : mutation_log(dir, max_log_file_mb, gpid, r),
          _batch_buffer_bytes(batch_buffer_bytes),
          _batch_buffer_max_count(batch_buffer_max_count),
          _batch_buffer_flush_interval_ms(batch_buffer_flush_interval_ms)
    {
        mutation_log_private::init_states();
    }

    virtual ~mutation_log_private() override { _tracker.cancel_outstanding_tasks(); }
    virtual ::dsn::task_ptr append(mutation_ptr &mu,
                                   dsn::task_code callback_code,
                                   dsn::task_tracker *tracker,
                                   aio_handler &&callback,
                                   int hash = 0,
                                   int64_t *pending_size = nullptr) override;

    virtual bool get_learn_state_in_memory(decree start_decree,
                                           binary_writer &writer) const override;

    virtual void flush() override;
    virtual void flush_once() override;

private:
    // async write pending mutations into log file
    // Preconditions:
    // - _pending_write != nullptr
    // - _issued_write.expired() == true (because only one async write is allowed at the same time)
    // release_lock_required should always be true => this function must release the lock
    // appropriately for less lock contention
    void write_pending_mutations(bool release_lock_required);

    virtual void init_states() override;

    // flush at most count times
    // if count <= 0, means flush until all data is on disk
    void flush_internal(int max_count);

    bool flush_interval_expired()
    {
        return _pending_write_start_time_ms + _batch_buffer_flush_interval_ms <= dsn_now_ms();
    }

private:
    // bufferring - only one concurrent write is allowed
    typedef std::vector<mutation_ptr> mutations;
    std::atomic_bool _is_writing;
    std::weak_ptr<mutations> _issued_write_mutations;
    std::shared_ptr<log_block> _pending_write;
    std::shared_ptr<mutations> _pending_write_mutations;
    int64_t _pending_write_start_offset;
    uint64_t _pending_write_start_time_ms;
    decree _pending_write_max_commit;
    decree _pending_write_max_decree;
    mutable zlock _plock;

    uint32_t _batch_buffer_bytes;
    uint32_t _batch_buffer_max_count;
    uint64_t _batch_buffer_flush_interval_ms;
};

//
// the log file is structured with sequences of log_blocks,
// each block consists of the log_block_header + log_content,
// and the first block contains the log_file_header at the beginning
//
// the class is not thread safe
//
class log_file : public ref_counter
{
public:
    ~log_file();

    //
    // file operations
    //

    // open the log file for read
    // 'path' should be in format of log.{index}.{start_offset}, where:
    //   - index: the index of the log file, start from 1
    //   - start_offset: start offset in the global space
    // returns:
    //   - non-null if open succeed
    //   - null if open failed
    static log_file_ptr open_read(const char *path, /*out*/ error_code &err);

    // open the log file for write
    // the file path is '{dir}/log.{index}.{start_offset}'
    // returns:
    //   - non-null if open succeed
    //   - null if open failed
    static log_file_ptr create_write(const char *dir, int index, int64_t start_offset);

    // close the log file
    void close();

    // flush the log file
    void flush() const;

    //
    // read routines
    //

    // sync read the next log entry from the file
    // the entry data is start from the 'local_offset' of the file
    // the result is passed out by 'bb', not including the log_block_header
    // return error codes:
    //  - ERR_OK
    //  - ERR_HANDLE_EOF
    //  - ERR_INCOMPLETE_DATA
    //  - ERR_INVALID_DATA
    //  - other io errors caused by file read operator
    error_code read_next_log_block(/*out*/ ::dsn::blob &bb);

    //
    // write routines
    //

    // prepare a log entry buffer, with block header reserved and inited
    // always returns non-nullptr
    static log_block *prepare_log_block();

    // async write log entry into the file
    // 'block' is the date to be written
    // 'offset' is start offset of the entry in the global space
    // 'evt' is to indicate which thread pool to execute the callback
    // 'callback_host' is used to get tracer
    // 'callback' is to indicate the callback handler
    // 'hash' helps to choose which thread in the thread pool to execute the callback
    // returns:
    //   - non-null if io task is in pending
    //   - null if error
    dsn::aio_task_ptr commit_log_block(log_block &block,
                                       int64_t offset,
                                       dsn::task_code evt,
                                       dsn::task_tracker *tracker,
                                       aio_handler &&callback,
                                       int hash);

    //
    // others
    //
    // reset file_streamer to point to the start of this log file.
    void reset_stream();
    // end offset in the global space: end_offset = start_offset + file_size
    int64_t end_offset() const { return _end_offset.load(); }
    // start offset in the global space
    int64_t start_offset() const { return _start_offset; }
    // file index
    int index() const { return _index; }
    // file path
    const std::string &path() const { return _path; }
    // previous decrees
    const replica_log_info_map &previous_log_max_decrees() { return _previous_log_max_decrees; }
    // previous decree for speicified gpid
    decree previous_log_max_decree(const gpid &pid);
    // file header
    log_file_header &header() { return _header; }

    // read file header from reader, return byte count consumed
    int read_file_header(binary_reader &reader);
    // write file header to writer, return byte count written
    int write_file_header(binary_writer &writer, const replica_log_info_map &init_max_decrees);
    // get serialized size of current file header
    int get_file_header_size() const;
    // if the file header is valid
    bool is_right_header() const;

    // set & get last write time, used for gc
    void set_last_write_time(uint64_t last_write_time) { _last_write_time = last_write_time; }
    uint64_t last_write_time() const { return _last_write_time; }

private:
    // make private, user should create log_file through open_read() or open_write()
    log_file(const char *path, disk_file *handle, int index, int64_t start_offset, bool is_read);

private:
    uint32_t _crc32;
    int64_t _start_offset; // start offset in the global space
    std::atomic<int64_t>
        _end_offset; // end offset in the global space: end_offset = start_offset + file_size
    class file_streamer;
    std::unique_ptr<file_streamer> _stream;
    disk_file *_handle;        // file handle
    bool _is_read;             // if opened for read or write
    std::string _path;         // file path
    int _index;                // file index
    log_file_header _header;   // file header
    uint64_t _last_write_time; // seconds from epoch time

    // this data is used for garbage collection, and is part of file header.
    // for read, the value is read from file header.
    // for write, the value is set by write_file_header().
    replica_log_info_map _previous_log_max_decrees;
};
}
} // namespace
