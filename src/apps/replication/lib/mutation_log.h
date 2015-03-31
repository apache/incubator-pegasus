/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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
typedef boost::intrusive_ptr<log_file> log_file_ptr;

typedef std::map<global_partition_id, decree, GlobalPartitionIDComparor> multi_partition_decrees;

class mutation_log : public virtual servicelet
{
public:
    // mutationPtr
    typedef std::function<void (mutation_ptr&)> ReplayCallback;

public:
    //
    // ctors 
    //
    mutation_log(uint32_t LogBufferSizeMB, uint32_t LogPendingMaxMilliseconds, uint32_t maxLogFileSizeInMB = (uint64_t) MAX_LOG_FILESIZE, bool batchWrite = true, int writeTaskNumber = 2);
    virtual ~mutation_log();
    
    //
    // initialization
    //
    int initialize(const char* dir);
    int replay(ReplayCallback callback);
    void reset();
    int start_write_service(multi_partition_decrees& initMaxDecrees, int maxStalenessForCommit);
    void close();
       
    //
    // log mutation
    //
    // return value: nullptr for error
    task_ptr append(mutation_ptr& mu, 
            task_code callback_code,
            servicelet* callback_host,
            aio_handler callback,
            int hash = -1);

    // Remove entry <gpid, decree> from m_initPreparedDecrees when a partition is removed. 
    void on_partition_removed(global_partition_id gpid);

    //
    //  garbage collection logs that are already covered by durable state on disk, return deleted log segment count
    //
    int garbage_collection(multi_partition_decrees& durable_decrees);

    //
    //    other inquiry routines
    const std::string& dir() const {return _dir;}
    int64_t              end_offset() const { return _global_end_offset; }
    int64_t              start_offset() const { return _global_start_offset; }

    std::map<int, log_file_ptr>& get_logfiles_for_test();

private:
    //
    //  internal helpers
    //
    typedef std::shared_ptr<std::list<aio_task_ptr>> pending_callbacks_ptr;

    int  create_new_log_file();
    void create_new_pending_buffer();    
    void internal_pending_write_timer(uint64_t id);
    static void internal_write_callback(error_code err, uint32_t size, pending_callbacks_ptr callbacks, utils::blob data);
    int  write_pending_mutations(bool create_new_log_when_necessary = true);

private:    
    
    zlock                     _lock;
    int64_t                     _max_log_file_size_in_bytes;            
    std::string               _dir;    
    bool                      _batch_write;

    // write & read
    int                       _last_file_number;
    std::map<int, log_file_ptr> _log_files;
    log_file_ptr                _last_log_file;
    log_file_ptr                _current_log_file;
    int64_t                     _global_start_offset;
    int64_t                     _global_end_offset;
    
    // gc
    multi_partition_decrees     _init_prepared_decrees;
    int                       _max_staleness_for_commit;

    // bufferring
    uint32_t                    _log_buffer_size_bytes;
    uint32_t                    _log_pending_max_milliseconds;
    
    message_ptr                _pending_write;
    pending_callbacks_ptr       _pending_write_callbacks;
    task_ptr                   _pending_write_timer;
    
    int                       _write_task_number;
};

class log_file : public ref_object
{
public:
    struct log_file_header
    {
        int32_t  magic;
        int32_t  version;
        int32_t  headerSize;
        int32_t  maxStalenessForCommit;
        int32_t  logBufferSizeBytes;
        int64_t  startGlobalOffset;
    };

public:    
    ~log_file() { close(); }

    //
    // file operations
    //
    static log_file_ptr opend_read(const char* path);
    static log_file_ptr create_write(const char* dir, int index, int64_t startOffset, int maxStalenessForCommit, int writeTaskNumber = 2);
    void close();

    //
    // read routines
    //
    int read_next_log_entry(__out_param dsn::utils::blob& bb);

    //
    // write routines
    //
    // return value: nullptr for error or immediate success (using ::GetLastError to get code), otherwise it is pending
    aio_task_ptr write_log_entry(
                    utils::blob& bb,
                    task_code evt,  // to indicate which thread pool to execute the callback
                    servicelet* callback_host,
                    aio_handler callback,
                    int64_t offset,
                    int hash
                    );
    
    // others
    int64_t end_offset() const { return _endOffset; }
    int64_t start_offset() const  { return _startOffset; }
    int   index() const { return _index; }
    const std::string& Path() const { return _path; }
    const multi_partition_decrees& InitPrepareDecrees() { return _init_prepared_decrees; }
    const log_file_header& header() const { return _header;}

    int  read_header(message_ptr& msg);
    int  write_header(message_ptr& msg, multi_partition_decrees& initMaxDecrees, int bufferSizeBytes);    
    
private:
    log_file(const char* path, handle_t handle, int index, int64_t startOffset, int maxStalenessForCommit, bool isRead, int writeTaskNumber = 2);

protected:        
    int64_t       _startOffset;
    int64_t       _endOffset;
    handle_t      _handle;
    bool        _isRead;
    std::string _path;
    int         _index;
    std::vector<aio_task_ptr>  _writeTasks;
    int                      _writeTaskItr;    

    // for gc
    multi_partition_decrees _init_prepared_decrees;    
    log_file_header         _header;
};

DEFINE_REF_OBJECT(log_file)

}} // namespace
