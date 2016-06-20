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

# include "replication_app_base.h"
# include "replica.h"
# include "mutation.h"
# include <dsn/internal/factory_store.h>
# include "mutation_log.h"
# include <fstream>
# include <sstream>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.app.base"

namespace dsn { namespace replication {

error_code replica_init_info::load(const char* file)
{
    std::ifstream is(file, std::ios::binary);
    if (!is.is_open())
    {
        derror("open file %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    magic = 0x0;
    is.read((char*)this, sizeof(*this));
    is.close();

    if (magic != 0xdeadbeef)
    {
        derror("data in file %s is invalid (magic)", file);
        return ERR_INVALID_DATA;
    }

    auto fcrc = crc;
    crc = 0; // set for crc computation
    auto lcrc = dsn_crc32_compute((const void*)this, sizeof(*this), 0);
    crc = fcrc; // recover

    if (lcrc != fcrc)
    {
        derror("data in file %s is invalid (crc)", file);
        return ERR_INVALID_DATA;
    }
    
    return ERR_OK;
}

error_code replica_init_info::store(const char* file)
{
    std::string ffile = std::string(file);
    std::string tmp_file = ffile + ".tmp";

    std::ofstream os(tmp_file.c_str(), (std::ofstream::out | std::ios::binary | std::ofstream::trunc));
    if (!os.is_open())
    {
        derror("open file %s failed", tmp_file.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    // compute crc
    crc = 0;
    crc = dsn_crc32_compute((const void*)this, sizeof(*this), 0);

    os.write((const char*)this, sizeof(*this));
    os.close();

    if (!utils::filesystem::rename_path(tmp_file, ffile))
    {
        derror("move file from %s to %s failed", tmp_file.c_str(), ffile.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

std::string replica_init_info::to_string()
{
    std::ostringstream oss;
    oss << "init_ballot = " << init_ballot
        << ", init_decree = " << init_decree
        << ", init_offset_in_shared_log = " << init_offset_in_shared_log
        << ", init_offset_in_private_log = " << init_offset_in_private_log;
    return oss.str();
}

error_code replica_app_info::load(const char* file)
{
    std::ifstream is(file, std::ios::binary);
    if (!is.is_open())
    {
        derror("open file %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    int64_t sz = 0;
    if (!::dsn::utils::filesystem::file_size(std::string(file), sz))
    {
        return ERR_FILE_OPERATION_FAILED;
    }

    std::shared_ptr<char> buffer(new char[sz]);
    is.read((char*)buffer.get(), sz);
    is.close();
    
    binary_reader reader(blob(buffer, sz));
    int magic;
    unmarshall(reader, magic, DSF_THRIFT_BINARY);

    if (magic != 0xdeadbeef)
    {
        derror("data in file %s is invalid (magic)", file);
        return ERR_INVALID_DATA;
    }

    unmarshall(reader, *_app, DSF_THRIFT_JSON);
    return ERR_OK;
}

error_code replica_app_info::store(const char* file)
{
    binary_writer writer;
    int magic = 0xdeadbeef;

    marshall(writer, magic, DSF_THRIFT_BINARY);
    marshall(writer, *_app, DSF_THRIFT_JSON);

    std::string ffile = std::string(file);
    std::string tmp_file = ffile + ".tmp";

    std::ofstream os(tmp_file.c_str(), (std::ofstream::out | std::ios::binary | std::ofstream::trunc));
    if (!os.is_open())
    {
        derror("open file %s failed", tmp_file.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    auto data = writer.get_buffer();
    os.write((const char*)data.data(), (std::streamsize)data.length());
    os.close();

    if (!utils::filesystem::rename_path(tmp_file, ffile))
    {
        return ERR_FILE_OPERATION_FAILED;
    }
    else
    {
        return ERR_OK;
    }
}

replication_app_base::replication_app_base(replica* replica)
{
    _dir_data = utils::filesystem::path_combine(replica->dir(), "data");
    _dir_learn = utils::filesystem::path_combine(replica->dir(), "learn");
    _replica = replica;
    _callbacks = replica->get_app_callbacks();

    install_perf_counters();
}

const char* replication_app_base::replica_name() const
{
    return _replica->name();
}

void replication_app_base::install_perf_counters()
{
    std::stringstream ss;
    
    ss << replica_name() << ".commit(#/s)";
    _app_commit_throughput.init("eon.app", ss.str().c_str(), COUNTER_TYPE_RATE, "commit throughput for current app");

    ss.clear();
    ss.str("");
    ss << replica_name() << ".latency(ns)";
    _app_commit_latency.init("eon.app", ss.str().c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, "commit latency for current app");

    ss.clear();
    ss.str("");
    ss << replica_name() << ".decree#";
    _app_commit_decree.init("eon.app", ss.str().c_str(), COUNTER_TYPE_NUMBER, "commit decree for current app");

    ss.clear();
    ss.str("");
    ss << replica_name() << ".req(#/s)";
    _app_req_throughput.init("eon.app", ss.str().c_str(), COUNTER_TYPE_RATE, "request throughput for current app");
}

void replication_app_base::reset_counters_after_learning()
{
    _app_commit_decree.set(last_committed_decree());
}

error_code replication_app_base::open_internal(replica* r, bool create_new)
{
    if (create_new)
    {
        auto dir = r->dir();
        if (!dsn::utils::filesystem::remove_path(dir))
        {
            derror("%s: remove replica dir %s failed", r->name(), dir.c_str());
            return ERR_FILE_OPERATION_FAILED;
        }
        if (!dsn::utils::filesystem::create_directory(dir))
        {
            derror("%s: create replica dir %s failed", r->name(), dir.c_str());
            return ERR_FILE_OPERATION_FAILED;
        }
    }
    
    if (!dsn::utils::filesystem::create_directory(_dir_data))
    {
        derror("%s: create data dir %s failed", r->name(), _dir_data.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    if (!dsn::utils::filesystem::create_directory(_dir_learn))
    {
        derror("%s: create learn dir %s failed", r->name(), _dir_learn.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    auto err = open();
    if (err == ERR_OK)
    {
        _last_committed_decree.store(last_durable_decree());
        if (!create_new)
        {
            std::string info_path = utils::filesystem::path_combine(r->dir(), ".info");
            err = _info.load(info_path.c_str());
            if (err == ERR_OK)
            {
                ddebug("%s: load replica_init_info succeed: %s", r->name(), _info.to_string().c_str());
            }
        }

        _app_commit_decree.add(last_committed_decree());
    }
    
    return err;
}

::dsn::error_code replication_app_base::open()
{
    auto gd = _replica->get_gpid();
    auto info = _replica->get_app_info();
    auto err = dsn_hosted_app_create(
            info->app_type.c_str(), gd, _dir_data.c_str(),
            &_app_context, &_app_context_callbacks);
    if (err == ERR_OK)
    {
        // TODO: setup info->envs
        char* argv[1];
        argv[0] = (char*)info->app_name.c_str();
        err = dsn_hosted_app_start(_app_context, 1, argv);
    }
    return err;
}

::dsn::error_code replication_app_base::close(bool clear_state)
{
    if (_app_context)
    {
        auto err = dsn_hosted_app_destroy(_app_context, clear_state);
        if (err == ERR_OK)
        {
            _last_committed_decree.store(0);
            _app_context = nullptr;
            _app_context_callbacks = nullptr;
        }

        return err;
    }
    else
    {
        return ERR_SERVICE_NOT_ACTIVE;
    }
}

::dsn::error_code replication_app_base::prepare_get_checkpoint(/*out*/ ::dsn::blob& learn_req)
{
    int capacity = 4096;
    void* buffer = dsn_transient_malloc(capacity);
    int occupied = 0;

    error_code err = _callbacks.calls.prepare_get_checkpoint(_app_context_callbacks, buffer, capacity, &occupied);
    while (err != ERR_OK)
    {
        dsn_transient_free(buffer);
        buffer = nullptr;

        if (err != ERR_CAPACITY_EXCEEDED)
        {
            derror("%s: call app.prepare_get_checkpoint() failed, err = %s", _replica->name(), err.to_string());
            break;
        }
        else
        {
            dwarn("%s: call app.prepare_get_checkpoint() returns ERR_CAPACITY_EXCEEDED, capacity = %s, need = %d",
                  _replica->name(), capacity, occupied);
            dassert(occupied > capacity, "");
            capacity = occupied;
            buffer = dsn_transient_malloc(capacity);
            err = _callbacks.calls.prepare_get_checkpoint(_app_context_callbacks, buffer, capacity, &occupied);
        }
    }

    if (err == ERR_OK)
    {
        learn_req = ::dsn::blob(
            std::shared_ptr<char>(
                (char*)buffer,
                [](char* buf) { dsn_transient_free((void*)buf); }),
            occupied
            );
    }

    return err;
}

::dsn::error_code replication_app_base::get_checkpoint(int64_t learn_start, const ::dsn::blob& learn_request, learn_state& state)
{
    int capacity = 4096;
    void* buffer = dsn_transient_malloc(capacity);
    dsn_app_learn_state* lstate = reinterpret_cast<dsn_app_learn_state*>(buffer);

    error_code err = _callbacks.calls.get_checkpoint(
        _app_context_callbacks, learn_start, last_committed_decree(),
        (void*)learn_request.data(), learn_request.length(), lstate, capacity);
    while (err != ERR_OK)
    {
        int need_size = lstate->total_learn_state_size;
        dsn_transient_free(buffer);
        buffer = nullptr;

        if (err != ERR_CAPACITY_EXCEEDED)
        {
            derror("%s: call app.get_checkpoint() failed, err = %s", _replica->name(), err.to_string());
            break;
        }
        else
        {
            dwarn("%s: call app.get_checkpoint() returns ERR_CAPACITY_EXCEEDED, capacity = %s, need = %d",
                  _replica->name(), capacity, need_size);
            dassert(need_size > capacity, "");
            capacity = need_size;
            buffer = dsn_transient_malloc(capacity);
            dsn_app_learn_state* lstate = reinterpret_cast<dsn_app_learn_state*>(buffer);
            err = _callbacks.calls.get_checkpoint(
                _app_context_callbacks, learn_start, last_committed_decree(),
                (void*)learn_request.data(), learn_request.length(), lstate, capacity);
        }
    }

    if (err == ERR_OK)
    {
        state.from_decree_excluded = lstate->from_decree_excluded;
        state.to_decree_included = lstate->to_decree_included;
        state.meta = dsn::blob(
            std::shared_ptr<char>((char*)buffer, [](char* buf) { dsn_transient_free((void*)buf); }),
            (const char*)lstate->meta_state_ptr - (const char*)buffer, 
            lstate->meta_state_size
            );

        for (int i = 0; i < lstate->file_state_count; i++)
        {
            state.files.push_back(std::string(*lstate->files++));
        }
    }

    return err;
}

::dsn::error_code replication_app_base::apply_checkpoint(dsn_chkpt_apply_mode mode, const learn_state& state)
{
    dsn_app_learn_state lstate;
    std::vector<const char*> files;

    lstate.from_decree_excluded = state.from_decree_excluded;
    lstate.to_decree_included = state.to_decree_included;
    lstate.meta_state_ptr = (void*)state.meta.data();
    lstate.meta_state_size = state.meta.length();
    lstate.file_state_count = (int)state.files.size();
    lstate.total_learn_state_size = 0;
    if (lstate.file_state_count > 0)
    {
        for (auto& f : state.files)
        {
            files.push_back(f.c_str());
        }

        lstate.files = &files[0];
    }

    error_code err = _callbacks.calls.apply_checkpoint(_app_context_callbacks, mode, last_committed_decree(), &lstate);
    if (err == ERR_OK)
    {
        dassert(lstate.to_decree_included == last_durable_decree(), "");
        _last_committed_decree.store(lstate.to_decree_included);
    }
    else
    {
        derror("%s: call app.apply_checkpoint() failed, err = %s", _replica->name(), err.to_string());
    }

    return err;
}

::dsn::error_code replication_app_base::write_internal(mutation_ptr& mu)
{
    dassert(mu->data.header.decree == last_committed_decree() + 1, "");
    dassert(mu->data.updates.size() == mu->client_requests.size(), "");
    dassert(mu->data.updates.size() > 0, "");

    int request_count = static_cast<int>(mu->client_requests.size());
    dsn_message_t* batched_requests = (dsn_message_t*)alloca(sizeof(dsn_message_t) * request_count);
    dsn_message_t* faked_requests = (dsn_message_t*)alloca(sizeof(dsn_message_t) * request_count);
    int batched_count = 0;
    int faked_count = 0;
    for (int i = 0; i < request_count; i++)
    {
        const mutation_update& update = mu->data.updates[i];
        dsn_message_t req = mu->client_requests[i];
        if (update.code != RPC_REPLICATION_WRITE_EMPTY)
        {
            dinfo("%s: mutation %s #%d: dispatch rpc call %s",
                    _replica->name(), mu->name(), i, update.code.to_string());
         
            if (req == nullptr)
            {
                req = dsn_msg_create_received_request(update.code, (dsn_msg_serialize_format)update.serialization_type,
                                                      (void*)update.data.data(), update.data.length());
                faked_requests[faked_count++] = req;
            }

            batched_requests[batched_count++] = req;
        }
        else
        {
            // empty mutation write
            dwarn("%s: mutation %s #%d: dispatch rpc call %s",
                    _replica->name(), mu->name(), i, update.code.to_string());
        }
    }

    // batch processing
    uint64_t start = dsn_now_ns();
    if (_callbacks.calls.on_batched_write_requests)
    {
        _callbacks.calls.on_batched_write_requests(_app_context_callbacks,
                                                   mu->data.header.decree,
                                                   batched_requests, batched_count);
    }   
    else
    {
        for (int i = 0; i < batched_count; i++)
        {
            dsn_hosted_app_commit_rpc_request(_app_context, batched_requests[i], true);
        }
    }
    uint64_t latency = dsn_now_ns() - start;

    // release faked requests
    for (int i = 0; i < faked_count; i++)
    {
        dsn_msg_release_ref(faked_requests[i]);
    }

    int perror = _callbacks.calls.get_physical_error(_app_context_callbacks);
    if (perror != 0)
    {
        derror("%s: mutation %s: get internal error %d",
               _replica->name(), mu->name(), perror);
        return ERR_LOCAL_APP_FAILURE;
    }

    ++_last_committed_decree;

    ddebug("%s: mutation %s committed, batched_count = %d",
           _replica->name(), mu->name(), batched_count);

    // TODO(qinzuoyan): check correction of statistics usage
    _replica->update_commit_statistics(1);
    _app_commit_decree.increment();
    _app_commit_throughput.add(1);
    _app_commit_latency.set(latency);
    _app_req_throughput.add(request_count);

    return ERR_OK;
}

::dsn::error_code replication_app_base::update_init_info(replica* r, int64_t shared_log_offset, int64_t private_log_offset)
{
    _info.crc = 0;
    _info.magic = 0xdeadbeef;
    _info.init_ballot = r->get_ballot();
    _info.init_decree = r->last_committed_decree();
    _info.init_offset_in_shared_log = shared_log_offset;
    _info.init_offset_in_private_log = private_log_offset;

    std::string info_path = utils::filesystem::path_combine(r->dir(), ".info");
    auto err = _info.store(info_path.c_str());
    if (err == ERR_OK)
    {
        ddebug("%s: store replica_init_info succeed: %s", r->name(), _info.to_string().c_str());
    }
    return err;
}

}} // end namespace
