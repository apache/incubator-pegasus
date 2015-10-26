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

#include "replication_common.h"
#include "mutation.h"

namespace dsn { namespace replication {

#define INVALID_FILENUMBER (0)
#define MAX_LOG_FILESIZE (32)

class log_file;
typedef dsn::ref_ptr<log_file> log_file_ptr;

struct log_replica_info
{
    decree  decree;
    int64_t log_start_offset;
};

typedef std::unordered_map<global_partition_id, decree>
    multi_partition_decrees;
typedef std::unordered_map<global_partition_id, log_replica_info>
    multi_partition_decrees_ex;

struct log_block_header
{
    int32_t magic;
    int32_t length;
    int32_t body_crc;
    uint32_t local_offset;
};

struct log_file_header
{
    int32_t  magic;
    int32_t  version;
    int32_t  header_size;
    int32_t  log_buffer_size_bytes;
    int64_t  start_global_offset;
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
        );

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

    typedef std::shared_ptr<std::list<::dsn::task_ptr>> pending_callbacks_ptr;
    void init_states();    
    error_code create_new_log_file();
    void create_new_pending_buffer();    
    static void internal_write_callback(error_code err, size_t size, pending_callbacks_ptr callbacks, blob data);
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
    std::shared_ptr<binary_writer> _pending_write;
    pending_callbacks_ptr          _pending_write_callbacks;

    // replica states
    bool                           _is_private;
    multi_partition_decrees        _shared_max_decrees;
    global_partition_id            _private_gpid;
    decree                         _private_max_decree;
};

class log_file : public ref_counter
{
public:    
    ~log_file() { close(); }

    //
    // file operations
    //
    static log_file_ptr open_read(const char* path);
    static log_file_ptr create_write(
        const char* dir, 
        int index, 
        int64_t start_offset
        );
    void close();

    //
    // read routines
    //
    error_code read_next_log_entry(int64_t local_offset, /*out*/::dsn::blob& bb);

    //
    // write routines
    //
    std::shared_ptr<binary_writer> prepare_log_entry();

    // return value: nullptr for error or immediate success (using ::GetLastError to get code), otherwise it is pending
    ::dsn::task_ptr commit_log_entry(
                    blob& bb,
                    int64_t offset,
                    dsn_task_code_t evt,  // to indicate which thread pool to execute the callback
                    clientlet* callback_host,
                    aio_handler callback,                    
                    int hash
                    );
    
    // others
    int64_t end_offset() const { return _end_offset; }
    int64_t start_offset() const  { return _start_offset; }
    int   index() const { return _index; }
    const std::string& path() const { return _path; }
    const multi_partition_decrees& previous_log_max_decrees() { return _previous_log_max_decrees; }
    log_file_header& header() { return _header;}

    int read_header(binary_reader& reader);
    int write_header(binary_writer& writer, multi_partition_decrees& init_max_decrees, int bufferSizeBytes);
    bool is_right_header() const;
    void flush();
    int get_file_header_size() const;
    
private:
    log_file(const char* path, dsn_handle_t handle, int index, int64_t start_offset, bool isRead);

private:        
    int64_t       _start_offset;
    int64_t       _end_offset;
    dsn_handle_t  _handle;
    bool          _is_read;
    std::string   _path;
    int           _index;

    // for gc
    multi_partition_decrees _previous_log_max_decrees;    
    log_file_header         _header;
};

}} // namespace
