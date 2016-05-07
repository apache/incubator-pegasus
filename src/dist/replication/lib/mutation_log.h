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

#include "replication_common.h"
#include "mutation.h"

namespace dsn { namespace replication {

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
    bool operator == (const replica_log_info& o) const
    {
        return max_decree == o.max_decree && valid_start_offset == o.valid_start_offset;
    }
};

typedef std::unordered_map<gpid, replica_log_info> replica_log_info_map;

// each block in log file has a log_block_header
struct log_block_header
{
    int32_t  magic; //0xdeadbeef
    int32_t  length; // block data length (not including log_block_header)
    int32_t  body_crc; // block data crc (not including log_block_header)
    uint32_t local_offset; // start offset of the block in this log file
};

// each log file has a log_file_header stored at the beginning of the first block's data content
struct log_file_header
{
    int32_t  magic; // 0xdeadbeef
    int32_t  version; // current 0x1
    int64_t  start_global_offset; // start offset in the global space, equals to the file name's postfix
};

// a memory structure holding data which belongs to one block.
class log_block
{
    std::vector<blob> _data; // the first blob is log_block_header
    size_t            _size; // total data size of all blobs
public:
    log_block(blob &&init_blob) : _data({init_blob}), _size(init_blob.length()) {}
    // get all blobs in the block
    const std::vector<blob>& data() const
    {
        return _data;
    }
    // get the first blob (which contains the log_block_header) from the block
    blob& front()
    {
        dassert(!_data.empty(), "trying to get first blob out of an empty log block");
        return _data.front();
    }
    // add a blob into the block
    void add(const blob& bb)
    {
        _size += bb.length();
        _data.push_back(bb);
    }
    // return total data size in the block
    size_t size() const
    {
        return _size;
    }
};

//
// manage a sequence of continuous mutation log files
// each log file name is: log.{index}.{global_start_offset}
//
// this class is thread safe
//
class mutation_log : public virtual clientlet, public ref_counter
{
public:
    // return true when the mutation's offset is not less than
    // the remembered (shared or private) valid_start_offset therefore valid for the replica
    typedef std::function<bool (mutation_ptr&)> replay_callback;

public:
    //
    // ctors 
    // when is_private = true, should specify "private_gpid"
    //
    mutation_log(
        const std::string& dir,
        int32_t batch_buffer_size_kb,
        int32_t max_log_file_mb,
        bool force_flush = false,
        bool is_private = false,
        gpid private_gpid = gpid(),
        replica* r = nullptr
        );
    virtual ~mutation_log();

    //
    // initialization
    //

    // open and replay
    // returns ERR_OK if succeed
    // not thread safe, but only be called when init
    error_code open(replay_callback callback);

    // close the log
    // thread safe
    void close();

    // flush the pending buffer
    // thread safe
    void flush();

    //
    // replay
    //
    static error_code replay(
        std::vector<std::string>& log_files,
        replay_callback callback,
        /*out*/ int64_t& end_offset
        );

    //
    // append
    //

    // append a log mutation
    // return value: nullptr for error
    // thread safe
    ::dsn::task_ptr append(mutation_ptr& mu,
            dsn_task_code_t callback_code,
            clientlet* callback_host,
            aio_handler callback,
            int hash = 0);

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

    // garbage collection for private log
    // remove log files if satisfy:
    //  - file.max_decree <= "durable_decree" || file.end_offset <= "valid_start_offset"
    //  - the current log file is excluded
    // thread safe
    int garbage_collection(gpid gpid, decree durable_decree, int64_t valid_start_offset);

    // garbage collection for shared log
    // remove log files if satisfy:
    //  - for each replica "r":
    //         r in not in file.max_decree
    //      || file.max_decree[r] <= gc_condition[r].max_decree
    //      || file.end_offset[r] <= gc_condition[r].valid_start_offset
    //  - the current log file should not be removed
    // thread safe
    int garbage_collection(replica_log_info_map& gc_condition);

    //
    //  when this is a private log, log files are learned by remote replicas
    //
    void get_learn_state(
        gpid gpid,
        ::dsn::replication::decree start,
        /*out*/ ::dsn::replication::learn_state& state
        ) const;

    //
    //  other inquiry routines
    //

    // log dir
    // thread safe (because nerver changed)
    const std::string& dir() const { return _dir; }
    
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

private:
    //
    //  internal helpers
    //
    static error_code replay(
        log_file_ptr log,
        replay_callback callback,
        /*out*/ int64_t& end_offset
        );

    static error_code replay(
        std::map<int, log_file_ptr>& log_files,
        replay_callback callback,
        /*out*/ int64_t& end_offset
        );

    // init memory states
    void init_states();

    // update max decree without lock
    void update_max_decree_no_lock(gpid gpid, decree d);

    // update max commit on disk without lock
    void update_max_commit_on_disk_no_lock(decree d);

    // create new log file and set it as the current log file
    // returns ERR_OK if create succeed
    // Preconditions:
    // - _pending_write == nullptr (because we need create new pending buffer to write file header)
    error_code create_new_log_file();

    // create new pending buffer for new block, will reserve block header
    // Preconditions:
    // - _pending_write == nullptr
    void create_new_pending_buffer();

    // async write pending mutations into log file
    // Preconditions:
    // - _pending_write != nullptr
    // - _issued_write.expired() == true (because only one async write is allowed at the same time)
    error_code write_pending_mutations(bool create_new_log_when_necessary = true);

    // callback of write_pending_mutations()
    typedef std::shared_ptr<std::list< ::dsn::task_ptr>> pending_callbacks_ptr;
    void internal_write_callback(error_code err,
                                 size_t size,
                                 log_file_ptr file,
                                 std::shared_ptr<log_block> block,
                                 pending_callbacks_ptr callbacks,
                                 decree max_commit,
                                 const std::vector<mutation_ptr>& mus);

private:
    std::string               _dir;
    bool                      _is_private;
    gpid                      _private_gpid; // only used for private log
    replica                   *_owner_replica; // only used for private log

    // options
    int64_t                   _max_log_file_size_in_bytes;    
    uint32_t                  _batch_buffer_bytes;
    bool                      _force_flush;

    ///////////////////////////////////////////////
    //// memory states
    ///////////////////////////////////////////////
    mutable zlock                  _lock;
    bool                           _is_opened;

    // logs
    int                            _last_file_index; // new log file index = _last_file_index + 1
    std::map<int, log_file_ptr>    _log_files; // index -> log_file_ptr
    log_file_ptr                   _current_log_file; // current log file
    int64_t                        _global_start_offset; // global start offset of all files
    int64_t                        _global_end_offset; // global end offset currently
    
    
    // bufferring
    volatile bool                  _is_writing;
    std::weak_ptr<log_block>       _issued_write;
    task_ptr                       _issued_write_task; // for debugging
    std::shared_ptr<log_block>     _pending_write;
    pending_callbacks_ptr          _pending_write_callbacks;
    std::vector<mutation_ptr>      _pending_write_mutations;
    decree                         _pending_write_max_commit; // only used for private log

    // replica log info
    // - log_info.max_decree: the max decree of mutations up to now
    // - log_info.valid_start_offset: the same with replica_init_info::init_offset

    // replica log info for shared log
    replica_log_info_map           _shared_log_info_map;

    // replica log info for private log
    replica_log_info               _private_log_info;
    decree                         _private_max_commit_on_disk; // the max last_committed_decree of written mutations up to now
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
    static log_file_ptr open_read(const char* path, /*out*/ error_code& err);

    // open the log file for write
    // the file path is '{dir}/log.{index}.{start_offset}'
    // returns:
    //   - non-null if open succeed
    //   - null if open failed
    static log_file_ptr create_write(const char* dir, int index, int64_t start_offset);

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
    error_code read_next_log_block(/*out*/::dsn::blob& bb);

    //
    // write routines
    //

    // prepare a log entry buffer, with block header reserved and inited
    // always returns non-nullptr
    std::shared_ptr<log_block> prepare_log_block() const;

    // async write log entry into the file
    // 'block' is the date to be writen
    // 'offset' is start offset of the entry in the global space
    // 'evt' is to indicate which thread pool to execute the callback
    // 'callback_host' is used to get tracer
    // 'callback' is to indicate the callback handler
    // 'hash' helps to choose which thread in the thread pool to execute the callback
    // returns:
    //   - non-null if io task is in pending
    //   - null if error
    ::dsn::task_ptr commit_log_block(
                    log_block& block,
                    int64_t offset,
                    dsn_task_code_t evt,
                    clientlet* callback_host,
                    aio_handler callback,                    
                    int hash
                    );

    //
    // others
    //
    // reset file_streamer to point to the start of this log file.
    void reset_stream();
    // end offset in the global space: end_offset = start_offset + file_size
    int64_t end_offset() const { return _end_offset; }
    // start offset in the global space
    int64_t start_offset() const  { return _start_offset; }
    // file index
    int index() const { return _index; }
    // file path
    const std::string& path() const { return _path; }
    // previous decrees
    const replica_log_info_map& previous_log_max_decrees() { return _previous_log_max_decrees; }
    // file header
    log_file_header& header() { return _header;}

    // read file header from reader, return byte count consumed
    int read_file_header(binary_reader& reader);
    // write file header to writer, return byte count written
    int write_file_header(binary_writer& writer, const replica_log_info_map& init_max_decrees);
    // get serialized size of current file header
    int get_file_header_size() const;
    // if the file header is valid
    bool is_right_header() const;
    
private:
    // make private, user should create log_file through open_read() or open_write()
    log_file(const char* path, dsn_handle_t handle, int index, int64_t start_offset, bool is_read);

private:        
    uint32_t         _crc32;
    int64_t          _start_offset; // start offset in the global space
    int64_t          _end_offset; // end offset in the global space: end_offset = start_offset + file_size
    class file_streamer;
    std::unique_ptr  <file_streamer> _stream;
    dsn_handle_t     _handle; // file handle
    bool             _is_read; // if opened for read or write
    std::string      _path; // file path
    int              _index; // file index
    log_file_header  _header; // file header

    // this data is used for garbage collection, and is part of file header.
    // for read, the value is read from file header.
    // for write, the value is set by write_file_header().
    replica_log_info_map _previous_log_max_decrees;
};

}} // namespace
