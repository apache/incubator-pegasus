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
    const std::string& dir,
    int32_t batch_buffer_size_kb,
    int32_t max_log_file_mb,
    bool is_private,
    global_partition_id private_gpid
    )
{
    _dir = dir;
    _is_private = is_private;
    _private_gpid = private_gpid;
    _max_log_file_size_in_bytes = static_cast<int64_t>(max_log_file_mb) * 1024L * 1024L;
    _batch_buffer_bytes = static_cast<uint32_t>(batch_buffer_size_kb) * 1024u;
    init_states();
}

void mutation_log::init_states()
{
    _is_opened = false;

    // logs
    _last_file_index = 0;
    _log_files.clear();
    _current_log_file = nullptr;
    _global_start_offset = 0;
    _global_end_offset = 0;

    // buffering
    _issued_write.reset();
    _pending_write.reset();
    _pending_write_callbacks = nullptr;
    _pending_write_max_commit = 0;

    // replica states
    _shared_log_info_map.clear();
    _private_log_info = {0, 0};
    _private_max_commit_on_disk = 0;
}

mutation_log::~mutation_log()
{
    close();
}

error_code mutation_log::open(replay_callback callback)
{
    dassert(!_is_opened, "cannot open a opened mutation_log");
    dassert(nullptr == _current_log_file, "the current log file must be null at this point");

    // create dir if necessary
    if (!dsn::utils::filesystem::path_exists(_dir))
    {
        if (!dsn::utils::filesystem::create_directory(_dir))
        {
            derror("open mutation_log: create log path failed");
            return ERR_FILE_OPERATION_FAILED;
        }
    }
    
    // load the existing logs
    _log_files.clear();

    std::vector<std::string> file_list;
    if (!dsn::utils::filesystem::get_subfiles(_dir, file_list, false))
    {
        derror("open mutation_log: get subfiles failed.");
        return ERR_FILE_OPERATION_FAILED;
    }

    if (nullptr == callback)
    {
        dassert(file_list.size() == 0, "log must be empty if callback is not present");
    }

    std::sort(file_list.begin(), file_list.end());

    error_code err = ERR_OK;
    for (auto& fpath : file_list)
    {
        log_file_ptr log = log_file::open_read(fpath.c_str(), err);
        if (log == nullptr)
        {
            if (err == ERR_HANDLE_EOF || err == ERR_INCOMPLETE_DATA || err == ERR_INVALID_PARAMETERS)
            {
                dwarn("skip file %s during log init, err = %s", fpath.c_str(), err.to_string());
                continue;
            }
            else
            {
                return err;
            }
        }

        ddebug("open log file %s succeed", fpath.c_str());

        dassert(_log_files.find(log->index()) == _log_files.end(), "");
        _log_files[log->index()] = log;
    }

    file_list.clear();

    // replay with the found files
    int64_t end_offset = 0;
    err = replay(
        _log_files,
        [this, callback](mutation_ptr& mu)
        {
            bool ret = true;

            if (callback)
            {
                ret = callback(mu); // actually replica::replay_mutation(mu, true|false);
            }

            if (ret)
            {
                this->update_max_decree_no_lock(mu->data.header.gpid, mu->data.header.decree);
                if (this->_is_private)
                {
                    this->update_max_commit_on_disk_no_lock(mu->data.header.last_committed_decree);
                }
            }

            return ret;
        },
        end_offset
        );

    if (ERR_OK == err)
    {
        _global_start_offset = _log_files.size() > 0 ? _log_files.begin()->second->start_offset() : 0;
        _global_end_offset = end_offset;
        _last_file_index = _log_files.size() > 0 ? _log_files.rbegin()->first : 0;
        _is_opened = true;
    }
    else
    {
        // clear
        for (auto& kv : _log_files)
        {
            kv.second->close();
        }
        _log_files.clear();
        init_states();
    }

    return err;
}

void mutation_log::close()
{
    {
        zauto_lock l(_lock);
        if (!_is_opened)
        {
            return;
        }
        _is_opened = false;
    }

    dinfo("close mutation log %s", dir().c_str());

    // make all data is on disk
    flush();

    {
        zauto_lock l(_lock);

        // close current log file
        if (nullptr != _current_log_file)
        {
            _current_log_file->close();
            _current_log_file = nullptr;
        }
    }

    // reset all states
    init_states();
}

void mutation_log::flush()
{
    // make sure previous writes are done
    if (!_issued_write.expired())
    {
        dsn_task_tracker_wait_all(tracker());
    }

    {
        zauto_lock _(_lock);
        if (_pending_write != nullptr)
        {
            write_pending_mutations();
        }
    }

    // make sure the latest flush writes are done
    if (!_issued_write.expired())
    {
        dsn_task_tracker_wait_all(tracker());
    }
}

error_code mutation_log::create_new_log_file()
{
    dassert(_pending_write == nullptr, "");
    dassert(_pending_write_callbacks == nullptr, "");
    if (_current_log_file != nullptr)
    {
        dassert(_current_log_file->end_offset() == _global_end_offset, "");
    }

    // create file
    log_file_ptr logf = log_file::create_write(
        _dir.c_str(),
        _last_file_index + 1,
        _global_end_offset
        );
    if (logf == nullptr)
    {
        derror ("cannot create log file with index %d", _last_file_index);
        return ERR_FILE_OPERATION_FAILED;
    }
    dassert(logf->end_offset() == logf->start_offset(), "");
    dassert(_global_end_offset == logf->end_offset(), "");
    ddebug("create new log file %s succeed", logf->path().c_str());

    // update states
    _last_file_index++;
    dassert(_log_files.find(_last_file_index) == _log_files.end(), "");
    _log_files[_last_file_index] = logf;

    // switch the current log file
    // the old log file may be hold by _log_files or aio_task
    _current_log_file = logf;

    // create new pending buffer because we need write file header
    create_new_pending_buffer();
    dassert(_global_end_offset == _current_log_file->start_offset() + sizeof(log_block_header), "");

    // write file header into pending buffer
    size_t header_len = 0;
    if (_is_private)
    {
        replica_log_info_map ds;
        ds[_private_gpid] = replica_log_info(_private_log_info.max_decree, _private_log_info.valid_start_offset);
        binary_writer temp_writer;
        header_len = logf->write_file_header(temp_writer, ds);
        _pending_write->add(temp_writer.get_buffer());
    }
    else
    {
        binary_writer temp_writer;
        header_len = logf->write_file_header(temp_writer, _shared_log_info_map);
        _pending_write->add(temp_writer.get_buffer());
    }
    dassert(_pending_write->size() == sizeof(log_block_header) + header_len, "");
    _global_end_offset += header_len;

    return ERR_OK;
}

void mutation_log::create_new_pending_buffer()
{
    dassert(_pending_write == nullptr, "");
    dassert(_pending_write_callbacks == nullptr, "");

    _pending_write = _current_log_file->prepare_log_block();
    _pending_write_callbacks.reset(new std::list< ::dsn::task_ptr>);
    _global_end_offset += _pending_write->data().front().length();
}

error_code mutation_log::write_pending_mutations(bool create_new_log_when_necessary)
{
    dassert(_pending_write != nullptr, "");
    dassert(_pending_write_callbacks != nullptr, "");
    dassert(_issued_write.expired(), "");

    uint64_t start_offset = _global_end_offset - _pending_write->size();
    bool new_log_file = create_new_log_when_necessary
        && (_global_end_offset - _current_log_file->start_offset() >= _max_log_file_size_in_bytes)
        ;

    task_ptr aio = _current_log_file->commit_log_block(
        *_pending_write,
        start_offset,
        LPC_WRITE_REPLICATION_LOG_FLUSH,
        this,
        std::bind(
            &mutation_log::internal_write_callback, this,
            std::placeholders::_1,
            std::placeholders::_2,
            _current_log_file,
            _pending_write,
            _pending_write_callbacks,
            _pending_write_max_commit),
        -1
        );

    if (aio == nullptr)
    {
        internal_write_callback(ERR_FILE_OPERATION_FAILED,
            0, _current_log_file, _pending_write, _pending_write_callbacks, _pending_write_max_commit);
        return ERR_FILE_OPERATION_FAILED;
    }
    else
    {
        dassert(_global_end_offset == _current_log_file->end_offset(), "");
    }

    _issued_write = _pending_write;
    _issued_write_task = aio;

    _pending_write = nullptr;
    _pending_write_callbacks = nullptr;
    _pending_write_max_commit = 0;

    if (new_log_file)
    {
        error_code ret = create_new_log_file();
        if (ret != ERR_OK)
        {
            derror("create new log file failed, err = %s", ret.to_string());
        }
        return ret;
    }

    return ERR_OK;
}

void mutation_log::internal_write_callback(
    error_code err,
    size_t size,
    log_file_ptr file,
    std::shared_ptr<log_block> block,
    mutation_log::pending_callbacks_ptr callbacks,
    decree max_commit
    )
{
    auto hdr = (log_block_header*)block->front().data();
    dassert(hdr->magic == 0xdeadbeef, "header magic is changed: 0x%x", hdr->magic);

    if (err == ERR_OK)
    {
        dassert(size == block->size(),
            "log write size must equal to the given size: %d vs %d",
            (int)size,
            block->size()
            );

        if(_is_private)
        {
            // update _private_max_commit_on_disk after writen into log file done
            update_max_commit_on_disk(max_commit);

            // TODO(qinzuoyan): why flush here?
            // FIXME : the file could have been closed
            file->flush();
        }
    }

    for (auto& cb : *callbacks)
    {
        cb->enqueue_aio(err, size);
    }
}

/*static*/ error_code mutation_log::replay(
    log_file_ptr log,
    replay_callback callback,
    /*out*/ int64_t& end_offset
    )
{
    end_offset = log->start_offset();
    dinfo("replay mutation log %s: offset = [%" PRId64 ", %" PRId64 ")",
        log->path().c_str(),
        log->start_offset(),
        log->end_offset()
        );

    ::dsn::blob bb;
    log->crc32() = 0;
    error_code err = log->read_next_log_block(0, bb);
    if (err != ERR_OK)
    {
        return err;
    }

    std::shared_ptr<binary_reader> reader(new binary_reader(bb));
    end_offset += sizeof(log_block_header);

    // read file header
    end_offset += log->read_file_header(*reader);
    if (!log->is_right_header())
    {
        return ERR_INVALID_DATA;
    }

    while (true)
    {
        while (!reader->is_eof())
        {
            auto old_size = reader->get_remaining_size();
            mutation_ptr mu = mutation::read_from(*reader, nullptr);
            dassert(nullptr != mu, "");
            mu->set_logged();

            if (mu->data.header.log_offset != end_offset)
            {
                derror("offset mismatch in log entry and mutation %" PRId64 " vs %" PRId64,
                    end_offset, mu->data.header.log_offset);
                err = ERR_INVALID_DATA;
                break;
            }

            callback(mu);

            end_offset += old_size - reader->get_remaining_size();
        }

        err = log->read_next_log_block(end_offset - log->start_offset(), bb);
        if (err != ERR_OK)
        {
            // if an error occurs in an log mutation block, then the replay log is stopped
            break;
        }

        reader.reset(new binary_reader(bb));
        end_offset += sizeof(log_block_header);
    }

    return err;
}

/*static*/ error_code mutation_log::replay(
    std::vector<std::string>& log_files,
    replay_callback callback,
    /*out*/ int64_t& end_offset
    )
{
    std::map<int, log_file_ptr> logs;
    for (auto& fpath : log_files)
    {
        error_code err;
        log_file_ptr log = log_file::open_read(fpath.c_str(), err);
        if (log == nullptr)
        {
            if (err == ERR_HANDLE_EOF || err == ERR_INCOMPLETE_DATA || err == ERR_INVALID_PARAMETERS)
            {
                dinfo("skip file %s during log replay", fpath.c_str());
                continue;
            }
            else
            {
                return err;
            }
        }

        dassert(logs.find(log->index()) == logs.end(), "");
        logs[log->index()] = log;
    }

    return replay(logs, callback, end_offset);
}

/*static*/ error_code mutation_log::replay(
    std::map<int, log_file_ptr>& logs,
    replay_callback callback,
    /*out*/ int64_t& end_offset
    )
{
    int64_t g_start_offset = 0;
    int64_t g_end_offset = 0;
    error_code err = ERR_OK;
    log_file_ptr last;
    int last_file_index = 0;

    if (logs.size() > 0)
    {
        g_start_offset = logs.begin()->second->start_offset();
        g_end_offset = logs.rbegin()->second->end_offset();
        last_file_index = logs.begin()->first - 1;
    }

    // check file index continuity
    for (auto& kv : logs)
    {
        if (++last_file_index != kv.first)
        {
            derror("log file missing with index %u", last_file_index);
            return ERR_OBJECT_NOT_FOUND;
        }
    }

    end_offset = g_start_offset;

    for (auto& kv : logs)
    {
        log_file_ptr& log = kv.second;

        if (log->start_offset() != end_offset)
        {
            derror("offset mismatch in log file offset and global offset %" PRId64 " vs %" PRId64,
                log->start_offset(), end_offset);
            return ERR_INVALID_DATA;
        }

        last = log;
        err = mutation_log::replay(log, callback, end_offset);

        log->close();

        if (err == ERR_OK || err == ERR_HANDLE_EOF)
        {
            // do nothing
        }
        else if (err == ERR_INCOMPLETE_DATA)
        {
            // If the file is not corrupted, it may also return the value of ERR_INCOMPLETE_DATA.
            // In this case, the correctness is relying on the check of start_offset.
            dwarn("delay handling error: %s", err.to_string());
        }
        else
        {
            // for other errors, we should break
            break;
        }
    }

    if (err == ERR_OK || err == ERR_HANDLE_EOF)
    {
        // the log may still be written when used for learning
        dassert(g_end_offset <= end_offset,
            "make sure the global end offset is correct: %" PRId64 " vs %" PRId64,
            g_end_offset,
            end_offset
            );
        err = ERR_OK;
    }
    else if (err == ERR_INCOMPLETE_DATA)
    {
        // ignore the last incomplate block
        err = ERR_OK;
    }
    else
    {
        // bad error
        derror("replay mutation log failed: %s", err.to_string());
    }

    return err;
}

decree mutation_log::max_decree(global_partition_id gpid) const
{
    zauto_lock l(_lock);
    if (_is_private)
    {
        dassert(gpid == _private_gpid, "replica gpid does not match");
        return _private_log_info.max_decree;
    }
    else
    {
        auto it = _shared_log_info_map.find(gpid);
        if (it != _shared_log_info_map.end())
            return it->second.max_decree;
        else
            return 0;
    }
}

decree mutation_log::max_commit_on_disk() const
{
    zauto_lock l(_lock);
    dassert(_is_private, "this method is only valid for private logs");
    return _private_max_commit_on_disk;
}

decree mutation_log::max_gced_decree(global_partition_id gpid, int64_t valid_start_offset) const
{
    check_valid_start_offset(gpid, valid_start_offset);

    zauto_lock l(_lock);

    if (_log_files.size() == 0)
    {
        if (_is_private)
            return _private_log_info.max_decree;
        else
        {
            auto it = _shared_log_info_map.find(gpid);
            if (it != _shared_log_info_map.end())
                return it->second.max_decree;
            else
                return 0;
        }
    }
    else
    {
        for (auto& log : _log_files)
        {
            // when invalid log exits, all new logs are preserved (not gced)
            if (valid_start_offset > log.second->start_offset())
                return 0;

            auto it = log.second->previous_log_max_decrees().find(gpid);
            if (it != log.second->previous_log_max_decrees().end())
            {
                return it->second.max_decree;
            }
            else
                return 0;
        }
        return 0;
    }
}

void mutation_log::check_valid_start_offset(global_partition_id gpid, int64_t valid_start_offset) const
{
    zauto_lock l(_lock);
    if (_is_private)
    {
        dassert(valid_start_offset == _private_log_info.valid_start_offset,
            "valid start offset mismatch: %" PRId64 " vs %" PRId64,
            valid_start_offset,
            _private_log_info.valid_start_offset
            );
    }
    else
    {
        auto it = _shared_log_info_map.find(gpid);
        if (it != _shared_log_info_map.end())
        {
            dassert(valid_start_offset == it->second.valid_start_offset,
                "valid start offset mismatch: %" PRId64 " vs %" PRId64,
                valid_start_offset,
                it->second.valid_start_offset
                );
        }
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
    error_code err = ERR_OK;

    dinfo("write replication log for mutation %s", mu->name());

    zauto_lock l(_lock);
    dassert(_is_opened, "");

    if (nullptr == _current_log_file)
    {
        err = create_new_log_file();
        dassert(
            ERR_OK == err,
            "create write log file failed, err = %s",
            err.to_string()
            );
    }

    // update meta data
    update_max_decree_no_lock(mu->data.header.gpid, d);

    //
    // write to buffer
    //
    if (_pending_write == nullptr)
    {
        create_new_pending_buffer();
    }

    mu->data.header.log_offset = _global_end_offset;
    mu->write_to_scatter([this](blob bb)
    {
        _pending_write->add(bb);
        _global_end_offset += bb.length();
    });

    task_ptr tsk = nullptr;
    if (callback)
    {
        tsk = new safe_task<aio_handler>(callback);
        tsk->add_ref(); // released in exec_aio
        dsn_task_t t = dsn_file_create_aio_task_ex(callback_code,
            safe_task<aio_handler>::exec_aio,
            safe_task<aio_handler>::on_cancel,
            tsk, hash, callback_host->tracker()
            );
        tsk->set_task_info(t);
        _pending_write_callbacks->push_back(tsk);
    }

    if (_is_private)
    {
        _pending_write_max_commit = std::max(_pending_write_max_commit,
                                             mu->data.header.last_committed_decree);
    }

    //
    // start to write
    //
    if (_issued_write.expired()) 
    {
        if (_batch_buffer_bytes == 0)
        {
            // not batching
            err = write_pending_mutations();
        }
        else
        {
            // batching
            if (static_cast<uint32_t>(_pending_write->size()) >= _batch_buffer_bytes)
            {
                // batch full, write now
                err = write_pending_mutations();
            }
            else
            {
                // batch not full, wait for batch write later
            }
        }
    }

    dassert(
        err == ERR_OK,
        "write pending mutation failed, err = %s",
        err.to_string()
        );

    return tsk;
}

void mutation_log::set_valid_start_offset_on_open(global_partition_id gpid, int64_t valid_start_offset)
{
    zauto_lock l(_lock);
    if (_is_private)
    {
        dassert(gpid == _private_gpid, "replica gpid does not match");
        _private_log_info.valid_start_offset = valid_start_offset;
    }
    else
    {
        _shared_log_info_map[gpid] = replica_log_info(0, valid_start_offset);
    }
}

int64_t mutation_log::on_partition_reset(global_partition_id gpid, decree max_decree)
{
    zauto_lock l(_lock);
    if (_is_private)
    {
        dassert(_private_gpid == gpid, "replica gpid does not match");
        _private_log_info.max_decree = max_decree;
        _private_log_info.valid_start_offset = _global_end_offset;
    }
    else
    {
        replica_log_info info(max_decree, _global_end_offset);
        auto it = _shared_log_info_map.insert(replica_log_info_map::value_type(gpid, info));
        if (!it.second)
        {
            dwarn("replica %d.%d has changed its max_decree from %" PRId64 " to %" PRId64 ", valid_start_offset from %" PRId64 " to %" PRId64,
                gpid.app_id, gpid.pidx,
                it.first->second.max_decree, info.max_decree,
                it.first->second.valid_start_offset, info.valid_start_offset
                );
            _shared_log_info_map[gpid] = info;
        }
    }
    return _global_end_offset;
}

void mutation_log::on_partition_removed(global_partition_id gpid)
{
    dassert(!_is_private, "this method is only valid for shared logs");
    zauto_lock l(_lock);
    _shared_log_info_map.erase(gpid);
}

void mutation_log::update_max_decree(global_partition_id gpid, decree d)
{
    zauto_lock l(_lock);
    update_max_decree_no_lock(gpid, d);
}

void mutation_log::update_max_decree_no_lock(global_partition_id gpid, decree d)
{
    if (!_is_private)
    {
        auto it = _shared_log_info_map.find(gpid);
        if (it != _shared_log_info_map.end())
        {
            if (it->second.max_decree < d)
            {
                it->second.max_decree = d;
            }
        }
        else
        {
            dassert(false, "replica has not been registered in the log before");
        }
    }
    else
    {
        dassert(gpid == _private_gpid, "replica gpid does not match");
        if (d > _private_log_info.max_decree)
        {
            _private_log_info.max_decree = d;
        }
    }
}

void mutation_log::update_max_commit_on_disk(decree d)
{
    zauto_lock l(_lock);
    update_max_commit_on_disk_no_lock(d);
}

void mutation_log::update_max_commit_on_disk_no_lock(decree d)
{
    dassert(_is_private, "this method is only valid for private logs");
    if (d > _private_max_commit_on_disk)
    {
        _private_max_commit_on_disk = d;
    }
}

void mutation_log::get_learn_state(
    global_partition_id gpid,
    ::dsn::replication::decree start,
    /*out*/ ::dsn::replication::learn_state& state
    ) const
{
    dassert(_is_private, "this method is only valid for private logs");
    dassert(_private_gpid == gpid, "replica gpid does not match");

    std::map<int, log_file_ptr> files;
    std::map<int, log_file_ptr>::reverse_iterator itr;
    log_file_ptr cfile = nullptr;

    {
        zauto_lock l(_lock);
        if (start > _private_log_info.max_decree)
            return;

        files = _log_files;
        cfile = _current_log_file;

        // also learn pending buffer
        std::vector<std::shared_ptr<log_block>> pending_buffers;
        if (auto locked_issued_ptr = _issued_write.lock())
        {
            pending_buffers.emplace_back(std::move(locked_issued_ptr));
        }
        if (_pending_write)
        {
            //we have a lock here, so no race
            pending_buffers.push_back(_pending_write);
        }

        binary_writer temp_writer;
        for (auto& block : pending_buffers) {
            dassert(!block->data().empty(), "log block can never be empty");
            auto hdr = (log_block_header*)block->front().data();

            //skip the block header
            auto bb_iterator = std::next(block->data().begin());
            
            if (hdr->local_offset == 0)
            {
                //we have a lock, so we are safe here
                dassert(bb_iterator != block->data().end(), "there is no file header in a local_offset=0 log block");
                //skip the file header
                ++bb_iterator;
            }
            for (; bb_iterator != block->data().end(); ++bb_iterator)
            {
                temp_writer.write(bb_iterator->data(), bb_iterator->length());
            }
        }
        state.meta.push_back(temp_writer.get_buffer());
    }

    // flush last file so learning can learn the on-disk state
    if (nullptr != cfile) cfile->flush();

    // find all applicable files
    bool skip_next = false;
    std::list<std::string> learn_files;
    for (itr = files.rbegin(); itr != files.rend(); ++itr)
    {
        log_file_ptr& log = itr->second;
        if (log->end_offset() <= _private_log_info.valid_start_offset)
            break;

        if (skip_next)
        {
            skip_next = (log->previous_log_max_decrees().size() == 0);
            continue;
        }

        if (log->end_offset() > log->start_offset())
        {
            // not empty file
            learn_files.push_back(log->path());
        }

        skip_next = (log->previous_log_max_decrees().size() == 0);
        
        // continue checking as this file may be a fault
        if (skip_next)
            continue;

        decree last_max_decree = log->previous_log_max_decrees().begin()->second.max_decree;

        // when all possible decrees are not needed
        if (last_max_decree < start)
        {
            // skip all older logs
            break;
        }
    }

    // reverse the order
    for (auto it = learn_files.rbegin(); it != learn_files.rend(); ++it)
    {
        state.files.push_back(*it);
    }
}

int mutation_log::garbage_collection(global_partition_id gpid, decree durable_decree, int64_t valid_start_offset)
{
    dassert(_is_private, "this method is only valid for private log");

    std::map<int, log_file_ptr> files;
    decree max_decree = invalid_decree;
    int current_file_index = -1;

    {
        zauto_lock l(_lock);
        files = _log_files;
        max_decree = _private_log_info.max_decree;
        if (_current_log_file != nullptr)
            current_file_index = _current_log_file->index();
    }

    if (files.size() <= 1)
    {
        // nothing to do
        return 0;
    }
    else 
    {
        // the last one should be the current log file
        dassert(current_file_index == -1 || files.rbegin()->first == current_file_index, "");
    }

    // find the largest file which can be deleted.
    // after iterate, the 'mark_it' will point to the largest file which can be deleted.
    std::map<int, log_file_ptr>::reverse_iterator mark_it;
    for (mark_it = files.rbegin(); mark_it != files.rend(); ++mark_it)
    {
        log_file_ptr log = mark_it->second;
        dassert(mark_it->first == log->index(), "");
        // currently, "max_decree" is the max decree covered by this log.

        // skip current file
        if (current_file_index == log->index())
        {
            // not break, go to update max decree
        }

        // log is invalid, ok to delete
        else if (valid_start_offset >= log->end_offset())
        {
            dinfo("gc @ %d.%d: max_offset for %s is %" PRId64 " vs %" PRId64 " as app.valid_start_offset.private,"
                " safe to delete this and all older logs",
                _private_gpid.app_id,
                _private_gpid.pidx,
                mark_it->second->path().c_str(),
                mark_it->second->end_offset(),
                valid_start_offset
                );
            break;
        }

        // all decrees are durable, ok to delete
        else if (durable_decree >= max_decree)
        {
            dinfo("gc @ %d.%d: max_decree for %s is %" PRId64 " vs %" PRId64 " as app.durable decree,"
                " safe to delete this and all older logs",
                _private_gpid.app_id,
                _private_gpid.pidx,
                mark_it->second->path().c_str(),
                max_decree,
                durable_decree
                );
            break;
        }

        // update max decree for the next log file
        auto& max_decrees = log->previous_log_max_decrees();
        auto it3 = max_decrees.find(gpid);
        dassert(it3 != max_decrees.end(), "impossible for private logs");
        max_decree = it3->second.max_decree;
    }

    if (mark_it == files.rend())
    {
        // no file to delete
        return 0;
    }

    // ok, let's delete files in increasing order of file index
    // to avoid making a hole in the file list
    int largest_to_delete = mark_it->second->index();
    int deleted = 0;
    for (auto it = files.begin(); it->second->index() <= largest_to_delete; ++it)
    {
        log_file_ptr log = it->second;
        dassert(it->first == log->index(), "");

        // close first
        log->close();

        // delete file
        auto& fpath = log->path();
        if (!dsn::utils::filesystem::remove_path(fpath))
        {
            derror("gc: fail to remove %s, stop current gc cycle ...", fpath.c_str());
            break;
        }

        // delete succeed
        ddebug("gc: log file %s is removed", fpath.c_str());
        deleted++;

        // erase from _log_files
        {
            zauto_lock l(_lock);
            _log_files.erase(it->first);
        }
    }

    return deleted;
}

int mutation_log::garbage_collection(replica_log_info_map& gc_condition)
{
    dassert(!_is_private, "this method is only valid for shared log");

    std::map<int, log_file_ptr> files;
    replica_log_info_map max_decrees;
    int current_file_index = -1;

    {
        zauto_lock l(_lock);
        files = _log_files;
        max_decrees = _shared_log_info_map;
        if (_current_log_file != nullptr)
            current_file_index = _current_log_file->index();
    }

    if (files.size() <= 1)
    {
        // nothing to do
        return 0;
    }
    else
    {
        // the last one should be the current log file
        dassert(-1 == current_file_index || files.rbegin()->first == current_file_index, "");
    }

    // find the largest file which can be deleted.
    // after iterate, the 'mark_it' will point to the largest file which can be deleted.
    std::map<int, log_file_ptr>::reverse_iterator mark_it;
    std::set<global_partition_id> kickout_replicas;
    for (mark_it = files.rbegin(); mark_it != files.rend(); ++mark_it)
    {
        log_file_ptr log = mark_it->second;
        dassert(mark_it->first == log->index(), "");

        bool delete_ok = true;

        // skip current file
        if (current_file_index == log->index())
        {
            delete_ok = false;
        }

        if (delete_ok)
        {
            for (auto& kv : gc_condition)
            {
                if (kickout_replicas.find(kv.first) != kickout_replicas.end())
                {
                    // no need to consider this replica
                    continue;
                }

                global_partition_id gpid = kv.first;
                decree gc_max_decree = kv.second.max_decree;
                int64_t valid_start_offset = kv.second.valid_start_offset;

                bool delete_ok_for_this_replica = false;
                bool kickout_this_replica = false;
                auto it3 = max_decrees.find(gpid);

                // log not found for this replica, ok to delete
                if (it3 == max_decrees.end())
                {
                    dassert(valid_start_offset >= log->end_offset(),
                        "valid start offset must be greater than the end of this log file");

                    dinfo("gc @ %d.%d: max_decree for %s is missing vs %" PRId64 " as gc max decree,"
                        " safe to delete this and all older logs for this replica",
                        gpid.app_id,
                        gpid.pidx,
                        log->path().c_str(),
                        gc_max_decree
                        );
                    delete_ok_for_this_replica = true;
                    kickout_this_replica = true;
                }

                // log is invalid for this replica, ok to delete
                else if (log->end_offset() <= valid_start_offset)
                {
                    dinfo("gc @ %d.%d: log is invalid for %s, as"
                        " valid start offset vs log end offset = %" PRId64 " vs %" PRId64 ", "
                        " it is therefore safe to delete this and all older logs for this replica",
                        gpid.app_id,
                        gpid.pidx,
                        log->path().c_str(),
                        valid_start_offset,
                        log->end_offset()
                        );
                    delete_ok_for_this_replica = true;
                    kickout_this_replica = true;
                }

                // all decrees are no more than gc max decree, ok to delete
                else if (it3->second.max_decree <= gc_max_decree)
                {
                    dinfo("gc @ %d.%d: max_decree for %s is %" PRId64 " vs %" PRId64 " as gc max decree,"
                        " it is therefore safe to delete this and all older logs for this replica",
                        gpid.app_id,
                        gpid.pidx,
                        log->path().c_str(),
                        it3->second.max_decree,
                        gc_max_decree
                        );
                    delete_ok_for_this_replica = true;
                    kickout_this_replica = true;
                }

                if (kickout_this_replica)
                {
                    // files before this file is useless for this replica,
                    // so from now on, this replica will not be considered anymore
                    kickout_replicas.insert(gpid);
                }

                if (!delete_ok_for_this_replica)
                {
                    // can not delete this file
                    delete_ok = false;
                    break;
                }
            }
        }

        if (delete_ok)
        {
            // found the largest file which can be deleted
            break;
        }

        // update max_decrees for the next log file
        max_decrees = log->previous_log_max_decrees();
    }

    if (mark_it == files.rend())
    {
        // no file to delete
        return 0;
    }

    // ok, let's delete files in increasing order of file index
    // to avoid making a hole in the file list
    int largest_to_delete = mark_it->second->index();
    int deleted = 0;
    for (auto it = files.begin(); it->second->index() <= largest_to_delete; ++it)
    {
        log_file_ptr log = it->second;
        dassert(it->first == log->index(), "");

        // close first
        log->close();

        // delete file
        auto& fpath = log->path();
        if (!dsn::utils::filesystem::remove_path(fpath))
        {
            derror("gc: fail to remove %s, stop current gc cycle ...", fpath.c_str());
            break;
        }

        // delete succeed
        ddebug("gc: log file %s is removed", fpath.c_str());
        deleted++;

        // erase from _log_files
        {
            zauto_lock l(_lock);
            _log_files.erase(it->first);
        }
    }

    return deleted;
}

//------------------- log_file --------------------------
/*static */log_file_ptr log_file::open_read(const char* path, /*out*/ error_code& err)
{
    char splitters[] = { '\\', '/', 0 };
    std::string name = utils::get_last_component(std::string(path), splitters);

    // log.index.start_offset
    if (name.length() < strlen("log.")
        || name.substr(0, strlen("log.")) != std::string("log."))
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }

    auto pos = name.find_first_of('.');
    dassert(pos != std::string::npos, "");
    auto pos2 = name.find_first_of('.', pos + 1);
    if (pos2 == std::string::npos)
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }

    /* so the log file format is log.index_str.start_offset_str */
    std::string index_str = name.substr(pos + 1, pos2 - pos - 1);
    std::string start_offset_str = name.substr(pos2 + 1);
    if (index_str.empty() || start_offset_str.empty())
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }

    char* p = nullptr;
    int index = static_cast<int>(strtol(index_str.c_str(), &p, 10));
    if (*p != 0)
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }
    int64_t start_offset = static_cast<int64_t>(strtoll(start_offset_str.c_str(), &p, 10));
    if (*p != 0)
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }

    dsn_handle_t hfile = dsn_file_open(path, O_RDONLY | O_BINARY, 0);
    if (!hfile)
    {
        err = ERR_FILE_OPERATION_FAILED;
        dwarn("open log file %s failed", path);
        return nullptr;
    }

    auto lf = new log_file(path, hfile, index, start_offset, true);
    blob hdr_blob;
    err = lf->read_next_log_block(0, hdr_blob);
    if (err == ERR_INVALID_DATA || err == ERR_INCOMPLETE_DATA || err == ERR_HANDLE_EOF || err == ERR_FILE_OPERATION_FAILED)
    {
        std::string removed = std::string(path) + ".removed";
        derror("read first log entry of file %s failed, err = %s. Rename the file to %s", path, err.to_string(), removed.c_str());
        delete lf;
        lf = nullptr;

        // rename file on failure
        dsn::utils::filesystem::rename_path(path, removed);

        return nullptr;
    }

    binary_reader reader(hdr_blob);
    lf->read_file_header(reader);
    if (!lf->is_right_header())
    {
        std::string removed = std::string(path) + ".removed";
        derror("invalid log file header of file %s. Rename the file to %s", path, removed.c_str());
        delete lf;
        lf = nullptr;

        // rename file on failure
        dsn::utils::filesystem::rename_path(path, removed);

        err = ERR_INVALID_DATA;
        return nullptr;
    }

    err = ERR_OK;
    return lf;
}

/*static*/ log_file_ptr log_file::create_write(
    const char* dir,
    int index,
    int64_t start_offset
    )
{
    char path[512]; 
    sprintf (path, "%s/log.%d.%" PRId64, dir, index, start_offset);

    if (dsn::utils::filesystem::path_exists(std::string(path)))
    {
        dwarn("log file %s already exist", path);
        return nullptr;
    }

    dsn_handle_t hfile = dsn_file_open(path, O_RDWR | O_CREAT | O_BINARY, 0666);
    if (!hfile)
    {
        dwarn("create log %s failed", path);
        return nullptr;
    }

    return new log_file(path, hfile, index, start_offset, false);
}

log_file::log_file(
    const char* path,
    dsn_handle_t handle,
    int index,
    int64_t start_offset,
    bool is_read
    )
{
    _start_offset = start_offset;
    _end_offset = start_offset;
    _handle = handle;
    _is_read = is_read;
    _path = path;
    _index = index;
    _crc32 = 0;
    memset(&_header, 0, sizeof(_header));

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
    if (_handle)
    {
        error_code err = dsn_file_close(_handle);
        dassert(
            err == ERR_OK,
            "dsn_file_close failed, err = %s",
            err.to_string()
            );

        _handle = nullptr;
    }
}

void log_file::flush() const
{
    dassert (!_is_read, "log file must be of write mode");

    if (_handle)
    {
        error_code err = dsn_file_flush(_handle);
        dassert(
            err == ERR_OK,
            "dsn_file_flush failed, err = %s",
            err.to_string()
            );
    }
}

error_code log_file::read_next_log_block(int64_t local_offset, /*out*/::dsn::blob& bb)
{
    dassert (_is_read, "log file must be of read mode");

    log_block_header hdr;
    task_ptr tsk = file::read(_handle, reinterpret_cast<char*>(&hdr), static_cast<int>(sizeof(log_block_header)),
        local_offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr);
    tsk->wait();

    auto read_count = tsk->io_size();
    auto err = tsk->error();
    if (err != ERR_OK || read_count != sizeof(log_block_header))
    {
        if (err == ERR_OK || err == ERR_HANDLE_EOF)
        {
            // if read_count is 0, then we meet the end of file
            err = (read_count == 0 ? ERR_HANDLE_EOF : ERR_INCOMPLETE_DATA);
        }
        else
        {
            derror("read data block header failed, size = %d vs %d, err = %s, local_offset = %" PRId64,
                read_count, (int)sizeof(log_block_header), err.to_string(), local_offset);
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

    tsk = file::read(_handle, const_cast<char*>(bb.data()), static_cast<int>(hdr.length),
        local_offset + read_count, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr);
    tsk->wait();

    read_count = tsk->io_size();
    err = tsk->error();
    if (err != ERR_OK || hdr.length != read_count)
    {
        derror("read data block body failed, size = %d vs %d, err = %s, local_offset = %" PRId64,
            read_count, (int)hdr.length, err.to_string(), local_offset);

        if (err == ERR_OK || err == ERR_HANDLE_EOF)
        {
            // because already read log_block_header above, so here must be imcomplete data
            err = ERR_INCOMPLETE_DATA;
        }

        return err;
    }

    auto crc = dsn_crc32_compute(static_cast<const void*>(bb.data()), static_cast<size_t>(hdr.length), _crc32);
    if (crc != hdr.body_crc)
    {
        derror("crc checking failed");
        return ERR_INVALID_DATA;
    }
    _crc32 = crc;

    return ERR_OK;
}

std::shared_ptr<log_block> log_file::prepare_log_block() const
{
    log_block_header hdr;
    hdr.magic = 0xdeadbeef;
    hdr.length = 0;
    hdr.body_crc = 0;
    hdr.local_offset = static_cast<uint32_t>(_end_offset - _start_offset);
    
    binary_writer temp_writer;
    temp_writer.write_pod(hdr);
    return std::shared_ptr<log_block>(new log_block(temp_writer.get_buffer()));
}

::dsn::task_ptr log_file::commit_log_block(
                log_block& block,
                int64_t offset,
                dsn_task_code_t evt,
                clientlet* callback_host,
                aio_handler callback,
                int hash
                )
{
    dassert(!_is_read, "log file must be of write mode");
    dassert(offset == end_offset(), "block start offset should match with file's end offset");
    dassert(block.size() > 0, "log_block can not be empty");

    int64_t local_offset = end_offset() - start_offset();
    auto hdr = reinterpret_cast<log_block_header*>(const_cast<char*>(block.front().data()));

    dassert(hdr->magic == 0xdeadbeef, "");
    dassert(hdr->local_offset == static_cast<uint32_t>(local_offset), "");

    hdr->length = static_cast<int32_t>(block.size() - sizeof(log_block_header));
    hdr->body_crc = _crc32;
    for (auto log_iter = std::next(block.data().begin()) ; log_iter != block.data().end(); ++ log_iter)
    {
        hdr->body_crc = dsn_crc32_compute(
            static_cast<const void*>(log_iter->data()),
            static_cast<size_t>(log_iter->length()), hdr->body_crc
            );
    }
    _crc32 = hdr->body_crc;

    std::unique_ptr<dsn_file_buffer_t> buffer_vector(new dsn_file_buffer_t[block.data().size()]);
    std::transform(block.data().begin(), block.data().end(), buffer_vector.get(), [](const blob& bb)
    {
        return dsn_file_buffer_t
            {
                reinterpret_cast<void*>(const_cast<char*>(bb.data())),
                bb.length()
            };
    });

    task_ptr task = file::write_vector(
        _handle,
        buffer_vector.get(),
        static_cast<int>(block.data().size()),
        static_cast<uint64_t>(local_offset),
        evt,
        callback_host,
        callback,
        hash
        );

    _end_offset += block.size();
    return task;
}

int log_file::read_file_header(binary_reader& reader)
{
    /*
     * the log file header structure:
     *   log_file_header +
     *   count + count * (gpid + replica_log_info)
     */
    reader.read_pod(_header);

    int count;
    reader.read(count);
    for (int i = 0; i < count; i++)
    {
        global_partition_id gpid;
        replica_log_info info;

        reader.read_pod(gpid);
        reader.read_pod(info);

        _previous_log_max_decrees[gpid] = info;
    }

    return get_file_header_size();
}

int log_file::get_file_header_size() const
{
    int count = static_cast<int>(_previous_log_max_decrees.size());
    return static_cast<int>(
        sizeof(log_file_header) + sizeof(count)
        + (sizeof(global_partition_id) + sizeof(replica_log_info))*count
        );
}

bool log_file::is_right_header() const
{
    return _header.magic == 0xdeadbeef &&
        _header.start_global_offset == _start_offset;
}

int log_file::write_file_header(
    binary_writer& writer,
    const replica_log_info_map& init_max_decrees
    )
{
    /*
     * the log file header structure:
     *   log_file_header +
     *   count + count * (gpid + replica_log_info)
     */
    _previous_log_max_decrees = init_max_decrees;

    _header.magic = 0xdeadbeef;
    _header.version = 0x1;
    _header.start_global_offset = start_offset();

    writer.write_pod(_header);

    int count = static_cast<int>(_previous_log_max_decrees.size());
    writer.write(count);
    for (auto& kv : _previous_log_max_decrees)
    {
        writer.write_pod(kv.first);
        writer.write_pod(kv.second);
    }

    return get_file_header_size();
}

}} // end namespace
