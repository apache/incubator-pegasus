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

#include "mutation_log.h"
#ifdef _WIN32
#include <io.h>
#endif

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "mutation_log"

namespace dsn { namespace replication {

    using namespace ::dsn::service;

mutation_log::mutation_log(
    uint32_t log_buffer_size_mb, 
    uint32_t log_pending_max_ms, 
    uint32_t max_log_file_mb, 
    bool batch_write
    )
{
    _log_buffer_size_bytes = log_buffer_size_mb * 1024 * 1024;
    _log_pending_max_milliseconds = log_pending_max_ms;    
    _max_log_file_size_in_bytes = ((int64_t)max_log_file_mb) * 1024L * 1024L;
    _batch_write = batch_write;
    _is_opened = false;
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

void mutation_log::reset_as_commit_log(global_partition_id gpid, decree lcd)
{
    close();

    _last_file_number = 0;
    _global_start_offset = 0;
    _global_end_offset = 0;

    _last_log_file = nullptr;
    _current_log_file = nullptr;

    _pending_write = nullptr;
    

    for (auto& kv : _log_files)
    {
        kv.second->close();
        utils::filesystem::remove_path(kv.second->path());
    }
    _log_files.clear();

    _previous_log_max_decrees.clear();
    _previous_log_max_decrees[gpid] = lcd;

    start_write_service(_previous_log_max_decrees, _max_staleness_for_commit);
}

error_code mutation_log::initialize(const char* dir)
{
    // zauto_lock l(_lock);

    //create dir if necessary
	if (!dsn::utils::filesystem::create_directory(dir))
    {
        derror ("open mutation_log: create log path failed");
        return ERR_FILE_OPERATION_FAILED;
    }

    _dir = std::string(dir);
    _last_file_number = 0;
    _log_files.clear();

	std::vector<std::string> file_list;
	if (!dsn::utils::filesystem::get_subfiles(dir, file_list, false))
	{
		derror("open mutation_log: get files failed.");
		return ERR_FILE_OPERATION_FAILED;
	}

	for (auto& fpath : file_list)
	{
		log_file_ptr log = log_file::open_read(fpath.c_str());
		if (log == nullptr)
		{
			dwarn("Skip file %s during log init", fpath.c_str());
			continue;
		}

		dassert(_log_files.find(log->index()) == _log_files.end(), "");
		_log_files[log->index()] = log;
	}
	file_list.clear();

    if (_log_files.size() > 0)
    {
        _last_file_number = _log_files.begin()->first - 1;
        _global_start_offset = _log_files.begin()->second->start_offset();
    }

    for (auto& kv : _log_files)
    {
        if (++_last_file_number != kv.first)
        {
            derror ("log file missing with index %u", _last_file_number);
            return ERR_OBJECT_NOT_FOUND;
        }

        _global_end_offset = kv.second->end_offset();
    }
    
    return ERR_OK;
}

error_code mutation_log::create_new_log_file()
{
    if (_current_log_file != nullptr)
    {
        _last_log_file = _current_log_file;
        dassert (_current_log_file->end_offset() == _global_end_offset, "");
    }

    log_file_ptr logf = log_file::create_write(
        _dir.c_str(),
        _last_file_number + 1, _global_end_offset, 
        _max_staleness_for_commit
        );

    if (logf == nullptr)
    {
        derror ("cannot create log file with index %d", _last_file_number);
        return ERR_FILE_OPERATION_FAILED;
    }    

    dinfo ("create new log file %s", logf->path().c_str());
    
    _last_file_number++;
    dassert (_log_files.find(_last_file_number) == _log_files.end(), "");
    _log_files[_last_file_number] = logf;

    dassert (logf->end_offset() == logf->start_offset(), "");
    dassert (_global_end_offset == logf->end_offset(), "");

    _current_log_file = logf; 

    create_new_pending_buffer();
    auto len = logf->write_header(
        *_pending_write, 
        _previous_log_max_decrees, 
        static_cast<int>(_log_buffer_size_bytes)
        );
    _global_end_offset += len;
    dassert (_pending_write->total_size() == len + sizeof(log_block_header), "");
    
    return ERR_OK;
}

void mutation_log::create_new_pending_buffer()
{
    dassert (_pending_write == nullptr, "");
    dassert (_pending_write_callbacks == nullptr, "");
    dassert (_pending_write_timer == nullptr, "");

    _pending_write_callbacks.reset(new std::list<::dsn::task_ptr>);
    _pending_write = _current_log_file->prepare_log_entry();
    _global_end_offset += _pending_write->total_size();
}

void mutation_log::internal_pending_write_timer(binary_writer* w_ptr)
{
    zauto_lock l(_lock);
    dassert (w_ptr == _pending_write.get(), "");
    
    _pending_write_timer = nullptr;
    auto err = write_pending_mutations();
    dassert(err == ERR_OK, 
        "write_pending_mutations failed, err = %s",
        err.to_string()
        );
}

error_code mutation_log::write_pending_mutations(bool create_new_log_when_necessary)
{
    dassert (_pending_write != nullptr, "");
    dassert (_pending_write_timer == nullptr, "");
    dassert (_pending_write_callbacks != nullptr, "");

    // write block header
    auto bb = _pending_write->get_buffer();    

    uint64_t offset = end_offset() - bb.length();
    bool new_log_file = create_new_log_when_necessary
        && (end_offset() - _current_log_file->start_offset()
        >= _max_log_file_size_in_bytes)
        ;
    
    auto aio = _current_log_file->commit_log_entry(
        bb,
        offset, 
        LPC_AIO_IMMEDIATE_CALLBACK, 
        this,
        std::bind(
            &mutation_log::internal_write_callback, 
            std::placeholders::_1, 
            std::placeholders::_2, 
            _pending_write_callbacks, bb),        
        -1
        );    
    
    if (aio == nullptr)
    {
        internal_write_callback(ERR_FILE_OPERATION_FAILED, 
            0, _pending_write_callbacks, bb);
    }
    else
    {
        dassert (_global_end_offset == _current_log_file->end_offset(), "");
    }

    _pending_write = nullptr;
    _pending_write_callbacks = nullptr;
    _pending_write_timer = nullptr;

    if (aio == nullptr)
    {
        return ERR_FILE_OPERATION_FAILED;
    }    

    if (new_log_file)
    {
        error_code ret = create_new_log_file();
        if (ret != ERR_OK)
        {
            dinfo ("create new log file failed, err = %s", ret.to_string());
        }
        return ret;
    }

    return ERR_OK;
}

void mutation_log::internal_write_callback(
    error_code err, 
    size_t size, 
    mutation_log::pending_callbacks_ptr callbacks,
    blob data
    )
{
    auto hdr = (log_block_header*)data.data();
    dassert(hdr->magic == 0xdeadbeef, "header magic is changed: 0x%x", hdr->magic);

    if (err == ERR_OK)
    {
        dassert((int)size == data.length(), 
            "log write size must equal to the given size: %d vs %d",
            (int)size,
            data.length()
            );
    }

    for (auto& cb : *callbacks)
    {
        cb->enqueue_aio(err, size);
    }
}

/*static*/ error_code mutation_log::replay(
    log_file_ptr log,
    replay_callback callback,
    /*out*/ int64_t& offset
    )
{
    offset = log->start_offset();

    ::dsn::blob bb;    
    error_code err = log->read_next_log_entry(0, bb);
    if (err != ERR_OK)
    {
        return err;
    }

    std::shared_ptr<binary_reader> reader(new binary_reader(bb));
    offset += sizeof(log_block_header);
    offset += log->read_header(*reader);
    if (!log->is_right_header())
        err = ERR_INVALID_DATA;

    while (true)
    {
        while (!reader->is_eof())
        {
            auto old_size = reader->get_remaining_size();
            mutation_ptr mu = mutation::read_from(*reader, nullptr);
            dassert(nullptr != mu, "");
            mu->set_logged();

            if (mu->data.header.log_offset != offset)
            {
                derror("offset mismatch in log entry and mutation %lld vs %lld",
                    offset, mu->data.header.log_offset);
                err = ERR_INVALID_DATA;
                break;
            }

            callback(mu);

            offset += old_size - reader->get_remaining_size();
        }
        
        err = log->read_next_log_entry(offset - log->start_offset(), bb);
        if (err != ERR_OK)
        {
            break;
        }

        reader.reset(new binary_reader(bb));
        offset += sizeof(log_block_header);
    }

    return err;
}

/*static*/ error_code mutation_log::replay(
    std::vector<std::string>& log_files,
    replay_callback callback,
    /*out*/ int64_t& offset
    )
{
    std::map<int, log_file_ptr> logs;
    for (auto& fpath : log_files)
    {
        log_file_ptr log = log_file::open_read(fpath.c_str());
        if (log == nullptr)
        {
            dwarn("Skip file %s during log init", fpath.c_str());
            continue;
        }

        dassert(logs.find(log->index()) == logs.end(), "");
        logs[log->index()] = log;
    }

    return replay(logs, callback, offset);
}


/*static*/ error_code mutation_log::replay(
    std::map<int, log_file_ptr>& logs,
    replay_callback callback,
    /*out*/ int64_t& offset
    )
{
    int64_t g_start_offset = 0, g_end_offset = 0;
    error_code err = ERR_OK;
    std::shared_ptr<binary_reader> reader;
    log_file_ptr last;
    
    if (logs.size() > 0)
    {
        g_start_offset = logs.begin()->second->start_offset();
        g_end_offset = logs.rbegin()->second->end_offset();
    }
        
    offset = g_start_offset;

    for (auto& kv : logs)
    {
        log_file_ptr& log = kv.second;

        if (log->start_offset() != offset)
        {
            derror("offset mismatch in log file offset and global offset %lld vs %lld",
                log->start_offset(), offset);
            return ERR_INVALID_DATA;
        }

        last = log;
        err = mutation_log::replay(log, callback, offset);

        log->close();

        if (err == ERR_INVALID_DATA || err == ERR_FILE_OPERATION_FAILED)
            break;
        else
        {
            // for other error codes, they are all checked by next loop or after loop
            dassert(err == ERR_OK
                || err == ERR_INCOMPLETE_DATA
                || err == ERR_HANDLE_EOF
                ,
                "unhandled error code: %s",
                err.to_string()
                );
        }
    }

    if (err == ERR_OK)
    {
        dassert(g_end_offset == offset,
            "make sure the global end offset is correct: %lld vs %lld",
            g_end_offset,
            offset
            );
    }
    else if (err == ERR_HANDLE_EOF || err == ERR_INCOMPLETE_DATA)
    {
        // incomplete tail block
        if (offset + last->header().log_buffer_size_bytes > g_end_offset)
        {
            err = ERR_OK;
        }

        // data lost
        else
        {
            err = ERR_INCOMPLETE_DATA;
        }

        // fix end offset so later log writing can be continued
        g_end_offset = offset;
    }
    else
    {
        // data failure
    }

    return err;
}

error_code mutation_log::replay(replay_callback callback)
{
    int64_t offset = 0;
    auto err = replay(_log_files, callback, offset);
    if (_log_files.size() > 0)
        _last_log_file = _log_files.rbegin()->second;

    _global_end_offset = offset;
    return err;
}

error_code mutation_log::start_write_service(
    multi_partition_decrees& init_max_decrees, 
    int max_staleness_for_commit
    )
{
    zauto_lock l(_lock);

    _previous_log_max_decrees = init_max_decrees;
    _max_staleness_for_commit = max_staleness_for_commit;
    _is_opened = true;
    
    dassert(_current_log_file == nullptr, "");
    return create_new_log_file();
}

decree mutation_log::max_decree(global_partition_id gpid) const
{
    zauto_lock l(_lock);

    auto it = _previous_log_max_decrees.find(gpid);
    if (it != _previous_log_max_decrees.end())
        return it->second;
    else
        return 0;
}

decree mutation_log::min_decree(global_partition_id gpid) const
{
    zauto_lock l(_lock);

    if (_log_files.size() == 0)
        return 0;
    else
    {
        auto it = _log_files.begin()->second->previous_log_max_decrees().find(gpid);
        if (it != _log_files.begin()->second->previous_log_max_decrees().end())
            return it->second;
        else
            return 0;
    }
}

void mutation_log::close()
{
    {
        zauto_lock l(_lock);
        _is_opened = false;
   
        if (_pending_write)
        {
            if (nullptr == _pending_write_timer)
            {
                write_pending_mutations();
            }
            else if (_pending_write_timer->cancel(true))
            {                
                _pending_write_timer = nullptr;
                write_pending_mutations(false);
            }
            _pending_write = nullptr;
        }
    }

    // make sure all issued aios are completed
    dsn_task_tracker_wait_all(tracker());

    // close last log file
    if (nullptr != _current_log_file)
    {
        _current_log_file->close();
        _current_log_file = nullptr;
    }
}

::dsn::task_ptr mutation_log::append(
                        mutation_ptr& mu,
                        dsn_task_code_t callback_code,
                        clientlet* callback_host,
                        aio_handler callback,
                        int hash
                        )
{
    auto d = mu->data.header.decree;

    zauto_lock l(_lock);
    dassert(_is_opened && nullptr != _current_log_file, "");

    auto it = _previous_log_max_decrees.find(mu->data.header.gpid);
    if (it != _previous_log_max_decrees.end())
    {
        if (it->second < d)
        {
            it->second = d;
        }
    }
    else
    {
        _previous_log_max_decrees[mu->data.header.gpid] = d;
    }

    if (_pending_write == nullptr)
    {
        create_new_pending_buffer();
    }

    auto old_size = _pending_write->total_size();
    mu->data.header.log_offset = end_offset();
    mu->write_to(*_pending_write);
    _global_end_offset += _pending_write->total_size() - old_size;

    task_ptr tsk = nullptr;

    if (callback)
    {
        tsk = new safe_task<aio_handler>(callback);
        tsk->add_ref(); // released in exec_aio
        dsn_task_t t = dsn_file_create_aio_task(callback_code,
            safe_task<aio_handler>::exec_aio, tsk, hash);
        tsk->set_task_info(t);
        _pending_write_callbacks->push_back(tsk);
    }
    
    error_code err = ERR_OK;
    if (!_batch_write)
    {
        err = write_pending_mutations();
    }
    else
    {
        if ((uint32_t)_pending_write->total_size() >= _log_buffer_size_bytes)
        {
            if (nullptr == _pending_write_timer)
            {
                err = write_pending_mutations();
            }   
            else if (_pending_write_timer->cancel(false))
            {
                _pending_write_timer = nullptr;
                err = write_pending_mutations();
            }
        }

        else if (nullptr == _pending_write_timer)
        {
            _pending_write_timer = tasking::enqueue(
                LPC_MUTATION_LOG_PENDING_TIMER,
                this,
                std::bind(&mutation_log::internal_pending_write_timer,
                    this, _pending_write.get()),
                -1,
                _log_pending_max_milliseconds
                );
        }
    }   

    dassert(
        err == ERR_OK,
        "write pending mutation failed, err = %s",
        err.to_string()
        );

    return tsk;
}

void mutation_log::on_partition_removed(global_partition_id gpid)
{
    zauto_lock l(_lock);
    _previous_log_max_decrees.erase(gpid);
}

void mutation_log::get_learn_state_when_as_commit_logs(
    global_partition_id gpid,
    ::dsn::replication::decree start,
    /*out*/ ::dsn::replication::learn_state& state
    )
{
    std::map<int, log_file_ptr> files;
    std::map<int, log_file_ptr>::reverse_iterator itr;
    log_file_ptr cfile = nullptr;

    {
        zauto_lock l(_lock);
        files = _log_files;
        cfile = _current_log_file;
    }

    bool skip_next = false;
    std::list<std::string> learn_files;
    for (itr = files.rbegin(); itr != files.rend(); itr++)
    {
        log_file_ptr& log = itr->second;
        if (skip_next)
        {
            skip_next = (log->previous_log_max_decrees().size() == 0);
            continue;
        }

        if (log->end_offset() > log->start_offset())
            learn_files.push_back(log->path());

        skip_next = (log->previous_log_max_decrees().size() == 0);
        if (skip_next)
            continue;

        decree last_commit = log->previous_log_max_decrees().begin()->second;

        // when all possible decress are not needed 
        if (last_commit < start)
        {
            // skip all older logs
            break;
        }

        dassert(log->previous_log_max_decrees().size() == 1
            && log->previous_log_max_decrees().begin()->first == gpid,
            "the log must be private in this case");
    }

    // reverse the order
    for (auto it = learn_files.rbegin(); it != learn_files.rend(); it++)
    {
        state.files.push_back(*it);
    }
}

int mutation_log::garbage_collection_when_as_commit_logs(
    global_partition_id gpid,
    decree durable_d
    )
{
    std::map<int, log_file_ptr> files;
    std::map<int, log_file_ptr>::reverse_iterator itr;

    {
        zauto_lock l(_lock);
        files = _log_files;
        if (nullptr != _current_log_file)
        {
            files.erase(_current_log_file->index());
        }
    }

    for (itr = files.rbegin(); itr != files.rend(); itr++)
    {
        log_file_ptr& log = itr->second;

        auto it3 = log->previous_log_max_decrees().find(gpid);
        if (it3 == log->previous_log_max_decrees().end())
        {
            // new partition, ok to delete older logs
            break;
        }
        else
        {
            decree last_commit = it3->second;

            // when all possible decress are covered by durable decress
            if (durable_d >= last_commit)
            {
                // ok to delete older logs
                break;
            }
        }
    }

    if (itr != files.rend()) itr++;

    int count = 0;
    for (; itr != files.rend(); itr++)
    {
        itr->second->close();

        auto& fpath = itr->second->path();
        ddebug("remove log segment %s", fpath.c_str());

        if (!dsn::utils::filesystem::remove_path(fpath))
        {
            derror("Fail to remove %s.", fpath.c_str());
        }

        count++;

        {
            zauto_lock l(_lock);
            _log_files.erase(itr->first);
        }
    }

    return count;
}

int mutation_log::garbage_collection(
        multi_partition_decrees& durable_decrees,
        multi_partition_decrees& max_seen_decrees
        )
{
    std::map<int, log_file_ptr> files;
    std::map<int, log_file_ptr>::reverse_iterator itr;
    
    {
        zauto_lock l(_lock);
        files = _log_files;
        if (nullptr != _current_log_file)
        {
            files.erase(_current_log_file->index());
        }
    }

    for (itr = files.rbegin(); itr != files.rend(); itr++)
    {
        log_file_ptr& log = itr->second;

        bool delete_older_filers = true;
        for (auto& kv : durable_decrees)
        {
            global_partition_id gpid = kv.first;
            decree last_durable_decree = kv.second;

            auto it4 = max_seen_decrees.find(gpid);
            dassert(it4 != max_seen_decrees.end(), "");

            decree max_seen_decree = it4->second;
            if (max_seen_decree < last_durable_decree)
            {
                // learn after last prepare, therefore it is ok to delete logs
                continue;
            }

            auto it3 = log->previous_log_max_decrees().find(gpid);
            if (it3 == log->previous_log_max_decrees().end())
            {
                // new partition, ok to delete older logs
            }
            else
            {
                decree init_prepare_decree_before_this = it3->second;
                decree max_prepared_decree_before_this =
                    init_prepare_decree_before_this + log->header().max_staleness_for_commit - 1;

                if (max_prepared_decree_before_this > max_seen_decree)
                    max_prepared_decree_before_this = max_seen_decree;
                
                // when all possible decress are covered by durable decress
                if (last_durable_decree >= max_prepared_decree_before_this)
                {
                    // ok to delete older logs
                }
                else
                {
                    delete_older_filers = false;
                    break;
                }
            }
        }

        if (delete_older_filers)
        {
            break;
        }
    }

    if (itr != files.rend()) itr++;
    
    int count = 0;
    for (; itr != files.rend(); itr++)
    {
        itr->second->close();

		auto& fpath = itr->second->path();
        ddebug("remove log segment %s", fpath.c_str());

		if (!dsn::utils::filesystem::remove_path(fpath))
		{
			derror("Fail to remove %s.", fpath.c_str());
		}

        count++;

        {
            zauto_lock l(_lock);
            _log_files.erase(itr->first);
        }
    }

    return count;
}

std::map<int, log_file_ptr>& mutation_log::get_logfiles_for_test()
{
    return _log_files;
}

//------------------- log_file --------------------------
/*static */log_file_ptr log_file::open_read(const char* path)
{
    std::string pt = std::string(path);
    char splitters[] = { '\\', '/', 0 };
    std::string name = utils::get_last_component(pt, splitters);

    // log.index.start_offset
    if (name.length() < strlen("log.")
        || name.substr(0, strlen("log.")) != std::string("log.")
        || (name.length() > strlen(".removed") 
        && name.substr(name.length() - strlen(".removed")) == std::string(".removed"))
        )
    {
        dwarn( "invalid log path %s", path);
        return nullptr;
    }

    auto pos = name.find_first_of('.');
    auto pos2 = name.find_first_of('.', pos + 1);
    if (pos2 == std::string::npos)
    {
        dwarn( "invalid log path %s", path);
        return nullptr;
    }

    dsn_handle_t hfile = (dsn_handle_t)(uintptr_t)dsn_file_open(path, O_RDONLY | O_BINARY, 0);

    if (hfile == 0)
    {
        dwarn("open log %s failed", path);
        return nullptr;
    }

    
    int index = atoi(name.substr(pos + 1, pos2 - pos - 1).c_str());
    int64_t start_offset = static_cast<int64_t>(atoll(name.substr(pos2 + 1).c_str()));
    
    auto lf = new log_file(path, hfile, index, start_offset, 0, true);
    blob hdr_blob;
    auto err = lf->read_next_log_entry(0, hdr_blob);
    if (err != ERR_OK)
    {
        derror("read first log entry of file %s failed, err = %s",
            lf->path().c_str(),
            err.to_string()
            );
        delete lf;
        return nullptr;
    }

    binary_reader reader(hdr_blob);
    lf->read_header(reader);

    return lf;
}

/*static*/ log_file_ptr log_file::create_write(
    const char* dir, 
    int index,
    int64_t start_offset, 
    int max_staleness_for_commit
    )
{
    char path[512]; 
    sprintf (path, "%s/log.%u.%lld", dir, index,
        static_cast<long long int>(start_offset));
    
    dsn_handle_t hfile = dsn_file_open(path, O_RDWR | O_CREAT | O_BINARY, 0666);
    if (hfile == 0)
    {
        dwarn("create log %s failed", path);
        return nullptr;
    }

    return new log_file(path, hfile, index, start_offset, max_staleness_for_commit, false);
}

log_file::log_file(
    const char* path, 
    dsn_handle_t handle, 
    int index, 
    int64_t start_offset, 
    int max_staleness_for_commit, 
    bool is_read
    )
{
    _start_offset = start_offset;
    _end_offset = start_offset;
    _handle = handle;
    _is_read = is_read;
    _path = path;
    _index = index; 
    memset(&_header, 0, sizeof(_header));
    _header.max_staleness_for_commit = max_staleness_for_commit;

    if (is_read)
    {
		int64_t sz;
		if (!dsn::utils::filesystem::file_size(_path, sz))
		{
			dassert(false, "fail to get file size of %s.", _path.c_str());
		}
		_end_offset += sz;
    }
}

void log_file::close()
{
    if (0 != _handle)
    {
        error_code err = dsn_file_close(_handle);
        dassert(
            err == ERR_OK,
            "dsn_file_close failed, err = %s",
            err.to_string()
            );

        _handle = 0;
    }
}

error_code log_file::read_next_log_entry(int64_t local_offset, /*out*/::dsn::blob& bb)
{
    dassert (_is_read, "log file must be of read mode");
    
    log_block_header hdr;    
    auto tsk = file::read(_handle, (char*)&hdr, (int)sizeof(log_block_header), 
        local_offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr);
    tsk->wait();

    auto read_count = tsk->io_size();
    auto err = tsk->error();
    if (err != ERR_OK || read_count != sizeof(log_block_header))
    {
        derror("read data failed, size = %d vs %d, err = %s, local_offset = %lld",
            read_count, (int)sizeof(log_block_header), err.to_string(), local_offset);

        if (err == ERR_HANDLE_EOF)
        {
            err = read_count == 0 ? ERR_HANDLE_EOF : ERR_INCOMPLETE_DATA;
        }

        return err;
    }

    if (hdr.magic != 0xdeadbeef)
    {
        derror("invalid data header magic: 0x%x", hdr.magic);
        return ERR_INVALID_DATA;
    }

    std::shared_ptr<char> data(new char[hdr.length]);
    bb.assign(data, 0, hdr.length);

    tsk = file::read(_handle, (char*)bb.data(), (int)hdr.length,
        local_offset + read_count, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr);
    tsk->wait();

    read_count = tsk->io_size();
    err = tsk->error();
    if (err != ERR_OK || hdr.length != read_count)
    {
        derror("read data failed, size = %d vs %d, err = %s, local_offset = %lld",
            read_count, (int)hdr.length, err.to_string(), local_offset);

        if (err == ERR_OK || err == ERR_HANDLE_EOF)
            err = ERR_INCOMPLETE_DATA;

        return err;
    }
    
    auto crc = dsn_crc32_compute((const void*)bb.data(), (size_t)hdr.length, 0);
    if (crc != hdr.body_crc)
    {
        derror("crc checking failed");
        return ERR_INVALID_DATA;
    }

    return ERR_OK;
}

std::shared_ptr<binary_writer> log_file::prepare_log_entry()
{
    auto writer = new binary_writer();
    writer->write_empty((int)sizeof(log_block_header));
    return std::shared_ptr<binary_writer>(writer);
}

::dsn::task_ptr log_file::commit_log_entry(
                blob& bb,
                int64_t offset,
                dsn_task_code_t evt,  // to indicate which thread pool to execute the callback
                clientlet* callback_host,
                aio_handler callback,
                int hash
                )
{
    dassert (!_is_read, "");
    dassert (offset == end_offset(), "");

    auto* hdr = (log_block_header*)bb.data();
    hdr->magic = 0xdeadbeef;
    hdr->length = bb.length() - sizeof(log_block_header);
    hdr->body_crc = dsn_crc32_compute(
        (const void*)(bb.data() + sizeof(log_block_header)),
        (size_t)hdr->length, 0
        );
    hdr->padding = 0;

    auto task = file::write(
        _handle, 
        bb.data(),
        bb.length(),
        offset - start_offset(), 
        evt, 
        callback_host,
        callback, 
        hash
        );
    
    _end_offset += bb.length();    
    return task;
}

int log_file::read_header(binary_reader& reader)
{
    reader.read_pod(_header);

    int count;
    reader.read(count);
    for (int i = 0; i < count; i++)
    {
        global_partition_id gpid;
        decree decree;
        
        reader.read_pod(gpid);
        reader.read(decree);

        _previous_log_max_decrees[gpid] = decree;
    }

    return static_cast<int>(
        sizeof(_header) + sizeof(count) 
        + (sizeof(global_partition_id) + sizeof(decree))*count
        );
}

bool log_file::is_right_header() const
{
    return _header.magic == 0xdeadbeef &&
        _header.start_global_offset == _start_offset;
}

int log_file::write_header(
    binary_writer& writer, 
    multi_partition_decrees& init_max_decrees, 
    int buffer_bytes
    )
{
    _previous_log_max_decrees = init_max_decrees;
    
    _header.magic = 0xdeadbeef;
    _header.version = 0x1;
    _header.start_global_offset = start_offset();
    _header.log_buffer_size_bytes = buffer_bytes;
    // staleness set in ctor

    writer.write_pod(_header);
    
    int count = static_cast<int>(_previous_log_max_decrees.size());
    writer.write(count);
    for (auto& kv : _previous_log_max_decrees)
    {
        writer.write_pod(kv.first);
        writer.write(kv.second);
    }

    return static_cast<int>(
        sizeof(_header)+sizeof(count)
        +(sizeof(global_partition_id)+sizeof(decree))*count
        );
}

}} // end namespace
