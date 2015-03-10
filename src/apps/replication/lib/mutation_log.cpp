/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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

#include "mutation_log.h"
#include <boost/filesystem.hpp>
#ifdef _WIN32
#include <io.h>
#endif

#define __TITLE__ "mutation_log"

namespace rdsn { namespace replication {

    using namespace ::rdsn::service;

mutation_log::mutation_log(uint32_t LogBufferSizeMB, uint32_t LogPendingMaxMilliseconds, uint32_t maxLogFileSizeInMB, bool batchWrite, int writeTaskNumber)
: service_base("mutation_log")
{
    _log_buffer_size_bytes = LogBufferSizeMB * 1024 * 1024;
    _log_pending_max_milliseconds = LogPendingMaxMilliseconds;    
    _max_log_file_size_in_bytes = ((int64_t)maxLogFileSizeInMB) * 1024L * 1024L;
    _batch_write = batchWrite;
    _write_task_number = writeTaskNumber;

    _last_file_number = 0;
    _global_start_offset = 0;
    _global_end_offset = 0;

    _last_log_file = nullptr;
    _current_log_file = nullptr;
    _dir = "";

    _max_staleness_for_commit = 0;
}

mutation_log::~mutation_log()
{
    close();
}

void mutation_log::reset()
{
    _last_file_number = 0;
    _global_start_offset = 0;
    _global_end_offset = 0;

    _last_log_file = nullptr;
    _current_log_file = nullptr;

    for (auto it = _log_files.begin(); it != _log_files.end(); it++)
    {
        it->second->close();
    }
    _log_files.clear();
}

int mutation_log::initialize(const char* dir)
{
    zauto_lock l(_lock);

    //create dir if necessary
    if (!boost::filesystem::exists(dir) && !boost::filesystem::create_directory(dir))
    {
        rerror("open mutation_log: create log path failed");
        return ERR_FILE_OPERATION_FAILED;
    }

    _dir = std::string(dir);

    
    _last_file_number = 0;
    _log_files.clear();

    boost::filesystem::directory_iterator endtr;

    for (boost::filesystem::directory_iterator it(dir);
        it != endtr;
        ++it)
    {
        std::string fullPath = it->path().string();
        log_file_ptr log = log_file::opend_read(fullPath.c_str());
        if (log == nullptr)
        {
            rwarn(
                "Skip file %s during log init", fullPath.c_str());
            continue;
        }

        rassert(_log_files.find(log->index()) == _log_files.end(), "");
        _log_files[log->index()] = log;
    }

    if (_log_files.size() > 0)
    {
        _last_file_number = _log_files.begin()->first - 1;
        _global_start_offset = _log_files.begin()->second->start_offset();
    }

    for (auto it = _log_files.begin(); it != _log_files.end(); it++)
    {
        if (++_last_file_number != it->first)
        {
            rerror(
                "log file missing with index %u", _last_file_number);
            return ERR_OBJECT_NOT_FOUND;
        }

        _global_end_offset = it->second->end_offset();
    }
    
    return ERR_SUCCESS;
}

int mutation_log::create_new_log_file()
{
    //rassert (_lock.IsHeldByCurrentThread(), "");

    if (_current_log_file != nullptr)
    {
        _last_log_file = _current_log_file;
        rassert(_current_log_file->end_offset() == _global_end_offset, "");
    }

    log_file_ptr logFile = log_file::create_write(_dir.c_str(), _last_file_number + 1, _global_end_offset, _max_staleness_for_commit, _write_task_number);
    if (logFile == nullptr)
    {
        rerror(
            "cannot create log file with index %u", _last_file_number);
        return ERR_FILE_OPERATION_FAILED;
    }    

    rerror(
        "create new log file %s", logFile->Path().c_str());
        
    _last_file_number++;
    rassert(_log_files.find(_last_file_number) == _log_files.end(), "");
    _log_files[_last_file_number] = logFile;

    rassert(logFile->end_offset() == logFile->start_offset(), "");
    rassert(_global_end_offset == logFile->end_offset(), "");

    _current_log_file = logFile; 

    create_new_pending_buffer();
    auto len = logFile->write_header(_pending_write, _init_prepared_decrees, (int)_log_buffer_size_bytes);
    _global_end_offset += len;

    return ERR_SUCCESS;
}

void mutation_log::create_new_pending_buffer()
{
    rassert(_pending_write == nullptr, "");
    rassert(_pending_write_callbacks == nullptr, "");
    rassert (_pending_write_timer == nullptr, "");

    _pending_write = message::create_request(RPC_PREPARE, _log_pending_max_milliseconds);
    _pending_write_callbacks.reset(new std::list<aio_task_ptr>);

    if (_batch_write)
    {
        _pending_write_timer = service_base::enqueue_task(
            LPC_MUTATION_LOG_PENDING_TIMER,
            this,
            std::bind(&mutation_log::internal_pending_write_timer, this, _pending_write->header().id),
            -1, 
            _log_pending_max_milliseconds
            );
    }

    rassert(_pending_write->total_size() == message_header::serialized_size(), "");
    _global_end_offset += message_header::serialized_size();
}

void mutation_log::internal_pending_write_timer(uint64_t id)
{
    zauto_lock l(_lock);
    rassert(nullptr != _pending_write, "");
    rassert (_pending_write->header().id == id, "");
    rassert (task::get_current_task() == _pending_write_timer, "");

    _pending_write_timer = nullptr;
    write_pending_mutations();
}

int mutation_log::write_pending_mutations(bool create_new_log_when_necessary)
{
    //rassert (_lock.IsHeldByCurrentThread(), "");
    rassert (_pending_write != nullptr, "");
    rassert(_pending_write_timer == nullptr, "");

    _pending_write->seal(true);
    auto bb = _pending_write->get_output_buffer();
    uint64_t offset = end_offset() - bb.length();
    auto buf = bb.buffer();
    utils::blob bb2(buf, bb.length());

    task_ptr aio = _current_log_file->write_log_entry(
        bb2,
        LPC_AIO_IMMEDIATE_CALLBACK,
        this,
        std::bind(
            &mutation_log::internal_write_callback, 
            std::placeholders::_1, 
            std::placeholders::_2, 
            _pending_write_callbacks, bb2),
        offset,
        -1
        );    
    
    if (aio == nullptr)
    {
        internal_write_callback(ERR_FILE_OPERATION_FAILED, 0, _pending_write_callbacks, bb2);
    }
    else
    {
        rassert(_global_end_offset == _current_log_file->end_offset(), "");
    }

    _pending_write = nullptr;
    _pending_write_callbacks = nullptr;
    _pending_write_timer = nullptr;

    if (aio == nullptr)
    {
        return ERR_FILE_OPERATION_FAILED;
    }    

    if (create_new_log_when_necessary && _current_log_file->end_offset() - _current_log_file->start_offset() >= _max_log_file_size_in_bytes)
    {
        int ret = create_new_log_file();
        if (ERR_SUCCESS != ret)
        {
            rerror("create new log file failed, err = %d", ret);
        }
        return ret;
    }
    return ERR_SUCCESS;
}

void mutation_log::internal_write_callback(error_code err, uint32_t size, mutation_log::pending_callbacks_ptr callbacks, utils::blob data)
{
    for (auto it = callbacks->begin(); it != callbacks->end(); it++)
    {
        (*it)->enqueue(err, size, 0, nullptr);
    }
}

/*
TODO: when there is a log error, the server cannot contain any primary or secondary any more!
*/
int mutation_log::replay(ReplayCallback callback)
{
    zauto_lock l(_lock);

    int64_t offset = start_offset();
    int err = ERR_SUCCESS;
    for (auto it = _log_files.begin(); it != _log_files.end(); it++)
    {
        log_file_ptr log = it->second;

        if (log->start_offset() != offset)
        {
            rerror("offset mismatch in log file offset and global offset %lld vs %lld", log->start_offset(), offset);
            return ERR_FILE_OPERATION_FAILED;
        }

        _last_log_file = log;

        rdsn::utils::blob bb;
        err = log->read_next_log_entry(bb);
        if (err != ERR_SUCCESS)
        {
            if (err == ERR_HANDLE_EOF)
            {
                err = ERR_SUCCESS;
                continue;
            }

            rerror(
                "read log header failed for %s, err = %x", log->Path().c_str(), err);
            break;
        }


        message_ptr msg(new message(bb));
        offset += message_header::serialized_size();

        if (!msg->is_right_body())
        {
            rerror(
                        "data read crc check failed at offset %llu", offset);
            return ERR_WRONG_CHECKSUM;
        }

        offset += log->read_header(msg);

        while (true)
        {
            while (!msg->is_eof())
            {
                auto oldSz = msg->get_remaining_size();
                mutation_ptr mu = mutation::read_from(msg);
                rassert(nullptr != mu, "");                                
                mu->set_logged();

                if (mu->data.header.logOffset != offset)
                {
                    rerror(
                        "offset mismatch in log entry and mutation %lld vs %lld", offset, mu->data.header.logOffset);
                    return ERR_FILE_OPERATION_FAILED;
                }

                callback(mu);

                offset += oldSz - msg->get_remaining_size();
            }

            err = log->read_next_log_entry(bb);
            if (err != ERR_SUCCESS)
            {
                if (err == ERR_HANDLE_EOF)
                {
                    err = ERR_SUCCESS;
                    break;
                }

                rerror(
                    "read log entry failed for %s, err = %x", log->Path().c_str(), err);
                break;
            }
            
            msg = new message(bb);
            offset += message_header::serialized_size();

            if (!msg->is_right_body())
            {
                rerror(
                            "data read crc check failed at offset %llu", offset);
                return ERR_WRONG_CHECKSUM;
            }
        }

        log->close();

        // tail data corruption is checked by next file's offset checking
        if (err != ERR_INVALID_DATA && err != ERR_SUCCESS)
            break;        
    }

    if (err == ERR_INVALID_DATA && offset + _last_log_file->header().logBufferSizeBytes >= end_offset())
    {
        // remove bad data at tail, but still we may lose data so error code remains unchanged
        _global_end_offset = offset;
    }
    else if (err == ERR_SUCCESS)
    {
        rassert(end_offset() == offset, "");
    }

    return err;
}

int mutation_log::start_write_service(multi_partition_decrees& initMaxDecrees, int maxStalenessForCommit)
{
    zauto_lock l(_lock);

    _init_prepared_decrees = initMaxDecrees;
    _max_staleness_for_commit = maxStalenessForCommit;
    
    rassert(_current_log_file == nullptr, "");
    return create_new_log_file();
}

void mutation_log::close()
{
    while (true)
    {
        zauto_lock l(_lock);

        if (nullptr != _pending_write_timer)
        {
            if (_pending_write_timer->cancel(false))
            {
                _pending_write_timer = nullptr;
                write_pending_mutations(false);
                rassert(nullptr == _pending_write_timer, "");
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(0));
                continue;
            }
        }

        if (nullptr != _current_log_file)
        {
            _current_log_file->close();
            _current_log_file = nullptr;
        }
        break;
    }
}

task_ptr mutation_log::append(mutation_ptr& mu, 
                        task_code callback_code,
                        service_base* callback_host,
                        aio_handler callback,
                        int hash)
{
    zauto_lock l(_lock);

    if (nullptr == _current_log_file) return nullptr;

    auto it = _init_prepared_decrees.find(mu->data.header.gpid);
    if (it != _init_prepared_decrees.end())
    {
        if (it->second < mu->data.header.decree)
        {
            it->second = mu->data.header.decree;
        }
    }
    else
    {
        _init_prepared_decrees[mu->data.header.gpid] = mu->data.header.decree;
    }
    
    if (_pending_write == nullptr)
    {
        create_new_pending_buffer();
    }

    auto oldSz = _pending_write->total_size();
    mu->data.header.logOffset = end_offset();
    mu->write_to(_pending_write);
    _global_end_offset += _pending_write->total_size() - oldSz;

    aio_task_ptr tsk(new service_aio_task(callback_code, callback_host, callback, hash));
    
    _pending_write_callbacks->push_back(tsk);

    /*if (rdsn::service::spec().traceOptions.PathTracing)
    {
        rdebug( 
            "BATCHTHROUGH mutation write with io callback DstTaskId = %016llx", task->TaskId()
                );
    }*/
    
    // printf ("append: %llu, offset = %llu, global = %llu, pendingSize = %u\n",
    //     mu->data.header.decree, mu->data.header.logOffset, _global_end_offset, _pending_write->total_size());

    if (!_batch_write)
    {
        write_pending_mutations();
    }
    else if ((uint32_t)_pending_write->total_size() >= _log_buffer_size_bytes)
    {
        if (_pending_write_timer->cancel(false))
        {
            _pending_write_timer = nullptr;
            write_pending_mutations();
        }
    }

    return tsk;
}

int mutation_log::garbage_collection(multi_partition_decrees& durable_decrees)
{
    std::map<int, log_file_ptr> files;
    std::map<int, log_file_ptr>::reverse_iterator itr;
    
    {
        zauto_lock l(_lock);
        files = _log_files;
        if (nullptr != _current_log_file) files.erase(_current_log_file->index());
    }
        
    for (itr = files.rbegin(); itr != files.rend(); itr++)
    {
        log_file_ptr log = itr->second;

        bool deleteOlderFiles = true;
        for (auto it2 = durable_decrees.begin(); it2 != durable_decrees.end(); it2++)
        {
            global_partition_id gpid = it2->first;
            decree lastDurableDecree = it2->second;
        
            auto it3 = log->InitPrepareDecrees().find(gpid);
            if (it3 == log->InitPrepareDecrees().end())
            {
                // new partition, ok to delete older logs
            }
            else
            {
                decree initPrepareDecree = it3->second;
                decree maxPrepareDecreeBeforeThis = initPrepareDecree + log->header().maxStalenessForCommit; // due to concurent prepare
                
                // when all possible decress are covered by durable decress
                if (lastDurableDecree >= maxPrepareDecreeBeforeThis)
                {
                    // ok to delete older logs
                }
                else
                {
                    deleteOlderFiles = false;
                    break;
                }
            }
        }

        if (deleteOlderFiles)
        {
            break;
        }
    }

    if (itr != files.rend()) itr++;
    
    int count = 0;
    for (; itr != files.rend(); itr++)
    {
        itr->second->close();

        rdebug(
            "remove log segment %s", itr->second->Path().c_str());

        std::string newName = itr->second->Path() + ".removed";
        
        boost::filesystem::rename(itr->second->Path().c_str(), newName.c_str());

        count++;

        {
        zauto_lock l(_lock);
        _log_files.erase(itr->first);
        }

        /*
        if (!::MoveFileExA(itr->second->Path().c_str(), nullptr, MOVEFILE_WRITE_THROUGH))
        {
            int err = GetLastError();
            LogR(log_level_Error, "mutation_log::truncate error. error: %d", err);
            return err;
        }
        */
    }

    return count;
}


std::map<int, log_file_ptr>& mutation_log::get_logfiles_for_test()
{
    return _log_files;
}


//------------------- log_file --------------------------
/*static */log_file_ptr log_file::opend_read(const char* path)
{
    std::string pt = std::string(path);
    auto pos = pt.find_last_of('/');
    if (pos == std::string::npos)
    {
        rwarn( "Invalid log path %s", path);
        return nullptr;
    }

    // log.index.startOffset
    std::string name = pt.substr(pos + 1);
    if (name.length() < strlen("log.")
        || name.substr(0, strlen("log.")) != std::string("log.")
        || (name.length() > strlen(".removed") && name.substr(name.length() - strlen(".removed")) == std::string(".removed"))
        )
    {
        rwarn( "Invalid log path %s", path);
        return nullptr;
    }

    pos = name.find_first_of('.');
    auto pos2 = name.find_first_of('.', pos + 1);
    if (pos2 == std::string::npos)
    {
        rwarn( "Invalid log path %s", path);
        return nullptr;
    }

    handle_t hFile = (handle_t)::open(path, O_RDONLY, 0);

    if (hFile == 0)
    {
        rwarn("open log %s failed", path);
        return nullptr;
    }

    
    int index = atoi(name.substr(pos + 1, pos2 - pos - 1).c_str());
    int64_t startOffset = atol(name.substr(pos2 + 1).c_str());
    
    return new log_file(path, hFile, index, startOffset, 0, true);
}

/*static*/ log_file_ptr log_file::create_write(const char* dir, int index, int64_t startOffset, int maxStalenessForCommit, int writeTaskNumber)
{
    char path[512]; 
    sprintf (path, "%s/log.%u.%llu", dir, index, startOffset);
    
    handle_t hFile = rdsn::service::file::open(path, O_RDWR | O_CREAT, 0);
    if (hFile == 0)
    {
        rwarn("create log %s failed", path);
        return nullptr;
    }

    return new log_file(path, hFile, index, startOffset, maxStalenessForCommit, false, writeTaskNumber);
}

log_file::log_file(const char* path, handle_t handle, int index, int64_t startOffset, int maxStalenessForCommit, bool isRead, int writeTaskNumber)
{
    _startOffset = startOffset;
    _endOffset = startOffset;
    _handle = handle;
    _isRead = isRead;
    _path = path;
    _index = index; 
    memset(&_header, 0, sizeof(_header));
    _header.maxStalenessForCommit = maxStalenessForCommit;
    _writeTaskItr = 0;    
    _writeTasks.resize(writeTaskNumber);

    if (isRead)
    {
        boost::filesystem::path cp(_path);
        _endOffset += boost::filesystem::file_size(cp);
    }
}

void log_file::close()
{
    for (size_t itr = 0; itr < _writeTasks.size(); ++itr)
    {
        if (_writeTasks.at(itr) != nullptr)
        {            
            _writeTasks.at(itr)->wait();
            _writeTasks.at(itr) = nullptr;
        }
    }

    if (0 != _handle)
    {
        if (_isRead)
            ::close((int)_handle);
        else
            rdsn::service::file::close(_handle);

        _handle = 0;
    }
}

int log_file::read_next_log_entry(__out_param rdsn::utils::blob& bb)
{
    rassert(_isRead, "");

    std::shared_ptr<char> hdrBuffer(new char[message_header::serialized_size()]);
    
    if (message_header::serialized_size() != ::read(
        (int)_handle,
        hdrBuffer.get(),
        message_header::serialized_size()
        ))
    {
        return ERR_FILE_OPERATION_FAILED;
    }

    message_header hdr;
    rdsn::utils::blob bb2(hdrBuffer, message_header::serialized_size());
    rdsn::utils::binary_reader reader(bb2);
    hdr.unmarshall(reader);

    if (!hdr.is_right_header((char*)hdrBuffer.get()))
    {
        rerror("invalid data header");
        return ERR_INVALID_DATA;
    }

    std::shared_ptr<char> data(new char[message_header::serialized_size() + hdr.body_length]);
    memcpy(data.get(), hdrBuffer.get(), message_header::serialized_size());
    bb.assign(data, 0, message_header::serialized_size() + hdr.body_length);

    if (hdr.body_length != ::read(
            (int)_handle,
            (void*)((char*)bb.data() + message_header::serialized_size()), 
            hdr.body_length
            ))
    {
        return ERR_FILE_OPERATION_FAILED;
    }
    
    return ERR_SUCCESS;
}

aio_task_ptr log_file::write_log_entry(
                utils::blob& bb,
                task_code evt,  // to indicate which thread pool to execute the callback
                service_base* callback_host,
                aio_handler callback,
                int64_t offset,
                int hash
                )
{
    rassert(!_isRead, "");
    rassert (offset == end_offset(), "");

    auto task = service_base::file_write(
        _handle, 
        bb.data(),
        bb.length(),
        offset - start_offset(), 
        evt, 
        callback_host,
        callback, 
        hash
        );
    
    _endOffset = offset + bb.length();

    //printf ("WriteBB: size = %u, startoffset = %llu, endOffset = %llu\n", bb.length(), offset, _endOffset);
        
    // !!! dangerous, we are in the middle of a local lock
    // we already have flow control on maximum on-the-fly prepare requests, so flow control here can be disabled
    /*if (_writeTasks.at(_writeTaskItr) != nullptr)
    {
        _writeTasks.at(_writeTaskItr)->wait();
    }

    _writeTasks.at(_writeTaskItr) = task;
    _writeTaskItr = (_writeTaskItr < (int)_writeTasks.size() - 1) ? _writeTaskItr + 1 : 0;*/

    return task;
}

int log_file::read_header(message_ptr& reader)
{
    
    reader->reader().read_pod(_header);

    int count;
    reader->read(count);
    for (int i = 0; i < count; i++)
    {
        global_partition_id gpid;
        decree decree;
        unmarshall(reader, gpid);
        reader->read(decree);

        _init_prepared_decrees[gpid] = decree;
    }

    return (int)sizeof(_header) + (int)sizeof(count) + (int)(sizeof(global_partition_id) + sizeof(decree))*(int)_init_prepared_decrees.size();
}

int log_file::write_header(message_ptr& writer, multi_partition_decrees& initMaxDecrees, int bufferSizeBytes)
{
    _init_prepared_decrees = initMaxDecrees;
    
    _header.magic = 0xdeadbeef;
    _header.version = 0x1;
    _header.startGlobalOffset = start_offset();
    _header.logBufferSizeBytes = bufferSizeBytes;
    // staleness set in ctor

    writer->writer().write_pod(_header);
    
    int count = (int)_init_prepared_decrees.size();
    writer->write(count);
    for (auto it = _init_prepared_decrees.begin(); it != _init_prepared_decrees.end(); it++)
    {
        marshall(writer, it->first);
        writer->write(it->second);
    }

    return (int)sizeof(_header) + (int)sizeof(count) + (int)(sizeof(global_partition_id) + sizeof(decree))*(int)_init_prepared_decrees.size();
}

}} // end namespace
