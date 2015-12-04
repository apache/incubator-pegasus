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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "replication_common.h"
#include "mutation.h"

namespace dsn { namespace replication {

#define INVALID_FILENUMBER (0)
#define MAX_LOG_FILESIZE (32)

class log_file;
typedef dsn::ref_ptr<log_file> log_file_ptr;

struct log_replica_info
{
    int64_t decree;
    int64_t log_start_offset;

    log_replica_info(int64_t d, int64_t o)
    {
        decree = d;
        log_start_offset = o;
    }

    log_replica_info()
    {
        decree = 0;
        log_start_offset = 0;
    }

    bool operator == (const log_replica_info& o) const
    {
        return decree == o.decree && log_start_offset == o.log_start_offset;
    }
};

typedef std::unordered_map<global_partition_id, decree>
    multi_partition_decrees;
typedef std::unordered_map<global_partition_id, log_replica_info>
    multi_partition_decrees_ex;

struct log_block_header
{
    int32_t magic;
    int32_t length; // block data length (not including log_block_header)
    int32_t body_crc; // block data crc (not including log_block_header)
    uint32_t local_offset; // start offset in the log file
};

struct log_file_header
{
    int32_t  magic;
    int32_t  version;
    int64_t  start_global_offset;
};

class log_block
{
    std::vector<blob> _data;
    size_t _size;
public:
    log_block(blob &&init_blob) : _data({init_blob}), _size(init_blob.length()) {}
    const std::vector<blob>& data() const
    {
        return _data;
    }
    blob& front()
    {
        dassert(!_data.empty(), "trying to get first blob out of an empty log block");
        return _data.front();
    }
    void add(const blob& bb)
    {
        _size += bb.length();
        _data.push_back(bb);
    }
    size_t size() const
    {
        return _size;
    };
};
class mutation_log : public virtual clientlet, public ref_counter
{
public:
    // return true when the mutation's offset is not less than
    // the remembered log_start_offset therefore valid for the replica
    typedef std::function<bool (mutation_ptr&)> replay_callback;

public:
    //
    // ctors 
    //
    mutation_log(
        const std::string& dir,
        bool is_private,
        uint32_t log_batch_buffer_MB,
        uint32_t max_log_file_mb        
        );
    virtual ~mutation_log();
    
    //
    // initialization
    //
    void set_valid_log_offset_before_open(global_partition_id gpid, int64_t valid_start_offset);

    // for shared
    error_code open(replay_callback callback);

    // for private
    error_code open(global_partition_id gpid, replay_callback callback, decree max_decree = invalid_decree);

    // 
    void close(bool clear_all = false);

    //
    // replay
    //
    static error_code replay(
        std::vector<std::string>& log_files,
        replay_callback callback,
        /*out*/ int64_t& end_offset
        );
           
    //
    // log mutation
    //
    // return value: nullptr for error
    ::dsn::task_ptr append(mutation_ptr& mu,
            dsn_task_code_t callback_code,
            clientlet* callback_host,
            aio_handler callback,
            int hash = 0);

    // add/remove entry <gpid, decree> from _previous_log_max_decrees
    // when a partition is added/removed. 
    void    on_partition_removed(global_partition_id gpid);
    // return current offset, needs to be remebered by caller for gc usage
    int64_t on_partition_reset(global_partition_id gpid, decree max_d);

    //
    //  garbage collection logs that are already covered by 
    //  durable state on disk, return deleted log segment count
    //
    int garbage_collection(multi_partition_decrees_ex& durable_decrees);

    int garbage_collection(global_partition_id gpid, decree durable_d, int64_t valid_start_offset);

    //
    //  when this is a private log, log files are learned by remote replicas
    //
    void get_learn_state(
        global_partition_id gpid,
        ::dsn::replication::decree start,
        /*out*/ ::dsn::replication::learn_state& state
        ) const;

    //
    //    other inquiry routines
    const std::string& dir() const {return _dir;}

    int64_t end_offset() const { zauto_lock l(_lock); return _global_end_offset; }
    
    // maximum decree so far
    decree max_decree(global_partition_id gpid) const;

    // maximum decree that is garbage collected
    decree max_gced_decree(global_partition_id gpid, int64_t valid_start_offset) const;
    
    // update
    void update_max_decrees(global_partition_id gpid, decree d);

    void check_log_start_offset(global_partition_id gpid, int64_t valid_start_offset) const;

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

    typedef std::shared_ptr<std::list< ::dsn::task_ptr>> pending_callbacks_ptr;
    void init_states();    
    error_code create_new_log_file();    
    void create_new_pending_buffer();    
    static void internal_write_callback(error_code err, size_t size, pending_callbacks_ptr callbacks, std::shared_ptr<log_block> logs, log_file_ptr file, bool is_private);
    error_code write_pending_mutations(bool create_new_log_when_necessary = true);
    
private:
    // options
    int64_t                   _max_log_file_size_in_bytes;    
    uint32_t                  _batch_buffer_bytes;

    // memory states
    std::string               _dir;
    bool                      _is_opened;

    // logs
    mutable zlock               _lock;
    int                         _last_file_number;
    std::map<int, log_file_ptr> _log_files;
    log_file_ptr                _current_log_file;
    int64_t                     _global_start_offset;
    int64_t                     _global_end_offset;
    
    
    // bufferring
    std::weak_ptr<log_block> _issued_write;
    std::shared_ptr<log_block> _pending_write;
    size_t _pending_write_size;
    pending_callbacks_ptr          _pending_write_callbacks;

    // replica states
    bool                           _is_private;
    multi_partition_decrees_ex     _shared_max_decrees;
    global_partition_id            _private_gpid;
    decree                         _private_max_decree;
    int64_t                        _private_valid_start_offset;
};

class log_file : public ref_counter
{
public:    
    ~log_file() { close(); }

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
    // file path is '{dir}/log.{index}.{start_offset}'
    // 'max_staleness_for_commit' will be write into the file header
    static log_file_ptr create_write(
        const char* dir, 
        int index, 
        int64_t start_offset
        );

    // close the log file
    void close();

    // flush the log file
    void flush() const;

    //
    // read routines
    //

    // sync read the next log entry from the file
    // the entry data is start from the 'local_offset' of the file
    // the result is passed out by 'bb'
    error_code read_next_log_block(int64_t local_offset, /*out*/::dsn::blob& bb);

    //
    // write routines
    //

    // prepare a log entry buffer, with block header reserved and inited
    std::shared_ptr<log_block> prepare_log_block() const;

    // async write log entry into the file
    // 'bb' is the date to be write
    // 'offset' is start offset of the entry in the global space
    // 'evt' is to indicate which thread pool to execute the callback
    // 'callback_host' is used to get tracer
    // 'callback' is to indicate the callback handler
    // 'hash' helps to choose which thread in the thread pool to execute the callback
    // returns:
    //   - non-null if io task is in pending
    //   - null if error
    ::dsn::task_ptr commit_log_block(
                    log_block& logs,
                    int64_t offset,
                    dsn_task_code_t evt,
                    clientlet* callback_host,
                    aio_handler callback,                    
                    int hash
                    );

    //
    // others
    //

    uint32_t& crc32() { return _crc32;  }
    // end offset in the global space: end_offset = start_offset + file_size
    int64_t end_offset() const { return _end_offset; }
    // start offset in the global space
    int64_t start_offset() const  { return _start_offset; }
    // file index
    int index() const { return _index; }
    // file path
    const std::string& path() const { return _path; }
    // previous decrees
    const multi_partition_decrees_ex& previous_log_max_decrees() { return _previous_log_max_decrees; }
    // file header
    log_file_header& header() { return _header;}

    // read file header from reader, return byte count consumed
    int read_header(binary_reader& reader);
    // write file header to writer, return byte count written
    int write_header(binary_writer& writer, multi_partition_decrees_ex& init_max_decrees, int bufferSizeBytes);
    // get serialized size of current file header
    int get_file_header_size() const;
    // if the file header is valid
    bool is_right_header() const;
    
private:
    log_file(const char* path, dsn_handle_t handle, int index, int64_t start_offset, bool isRead);

private:        
    uint32_t      _crc32;
    int64_t       _start_offset;
    int64_t       _end_offset;
    dsn_handle_t  _handle;
    bool          _is_read;
    std::string   _path;
    int           _index;

    // for gc
    multi_partition_decrees_ex _previous_log_max_decrees;    
    log_file_header            _header;
};

}} // namespace
