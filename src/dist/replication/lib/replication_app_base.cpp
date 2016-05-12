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

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.app_base"

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
        return ERR_FILE_OPERATION_FAILED;
    }

    ddebug("update app init info in %s, ballot = %" PRId64 ", decree = %" PRId64 ", log_offset<S,P> = <%" PRId64 ",%" PRId64 ">",
        ffile.c_str(),
        init_ballot,
        init_decree,
        init_offset_in_shared_log,
        init_offset_in_private_log
        );
    return ERR_OK;
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
    _dir_data = replica->dir() + "/data";
    _dir_learn = replica->dir() + "/learn";
    _last_committed_decree = 0;
    _replica = replica;

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
        auto& dir = data_dir();
        dsn::utils::filesystem::remove_path(dir);
        dsn::utils::filesystem::create_directory(dir);
    }
    
    if (!dsn::utils::filesystem::create_directory(_dir_data))
    {
        dassert(false, "Fail to create directory %s.", _dir_data.c_str());
    }

    if (!dsn::utils::filesystem::create_directory(_dir_learn))
    {
        dassert(false, "Fail to create directory %s.", _dir_learn.c_str());
    }

    auto err = open();
    if (err == ERR_OK)
    {
        _last_committed_decree = last_durable_decree();
        if (!create_new)
        {
            std::string info_path = utils::filesystem::path_combine(r->dir(), ".info");
            err = _info.load(info_path.c_str());
        }

        _app_commit_decree.add(last_committed_decree());
    }
    
    return err;
}

::dsn::error_code replication_app_base::open()
{
    auto gd = _replica->get_gpid();
    ::dsn::error_code err = dsn_layer1_app_create(gd, &_app_context);
    if (err == ERR_OK)
    {
        _dir_data = dsn_get_current_app_data_dir(gd);
        err = dsn_layer1_app_start(_app_context);
    }
    return err;
}

void replication_app_base::prepare_get_checkpoint(/*out*/ ::dsn::blob& learn_req)
{
    int size = 4096;
    void* buffer = dsn_transient_malloc(size);
    int sz2 = dsn_layer1_app_prepare_learn_request(_app_context, buffer, size);

    while (sz2 > size)
    {
        dsn_transient_free(buffer);

        size = sz2;
        buffer = dsn_transient_malloc(size);
        sz2 = dsn_layer1_app_prepare_learn_request(_app_context, buffer, size);
    }

    if (sz2 == 0)
    {
        dsn_transient_free(buffer);
    }
    else
    {
        learn_req = ::dsn::blob(
            std::shared_ptr<char>(
                (char*)buffer,
                [](char* buf) { dsn_transient_free((void*)buf); }),
            sz2
            );
    }
}

::dsn::error_code replication_app_base::get_checkpoint(
    int64_t start,
    const ::dsn::blob& learn_request,
    learn_state& state
    )
{
    dsn_app_learn_state* lstate;
    int size = 4096;
    void* buffer = dsn_transient_malloc(size);
    lstate = (dsn_app_learn_state*)buffer;

    error_code err = dsn_layer1_app_get_checkpoint(_app_context, start,
        _last_committed_decree.load(),
        (void*)learn_request.data(),
        learn_request.length(),
        (dsn_app_learn_state*)buffer,
        size
        );

    while (err == ERR_CAPACITY_EXCEEDED)
    {
        dsn_transient_free(buffer);

        size = lstate->total_learn_state_size;
        buffer = dsn_transient_malloc(size);
        lstate = (dsn_app_learn_state*)buffer;

        err = dsn_layer1_app_get_checkpoint(_app_context, start,
            _last_committed_decree.load(),
            (void*)learn_request.data(),
            learn_request.length(),
            (dsn_app_learn_state*)buffer,
            size
            );
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
    else
    {
        dsn_transient_free(buffer);
    }
    return err;
}

::dsn::error_code replication_app_base::apply_checkpoint(const learn_state& state, dsn_chkpt_apply_mode mode)
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

    auto lcd = last_committed_decree();
    auto err = dsn_layer1_app_apply_checkpoint(_app_context, _last_committed_decree.load(), &lstate, mode);
    if (err == ERR_OK)
    {
        if (lstate.to_decree_included > lcd)
        {
            _last_committed_decree.store(lstate.to_decree_included);
        }
    }
    return err;
}

error_code replication_app_base::write_internal(mutation_ptr& mu)
{
    dassert(mu->data.header.decree == last_committed_decree() + 1, "");
    dassert(mu->data.updates.size() == mu->client_requests.size(), "");
    dassert(mu->data.updates.size() > 0, "");

    int count = static_cast<int>(mu->client_requests.size());
    //auto& bs = _app_info->info.type1.batch_state;
    //bs = (count == 1 ? BS_NOT_BATCH : BS_BATCH);
    for (int i = 0; i < count; i++)
    {
        /*if (bs == BS_BATCH && i + 1 == count)
        {
            bs = BS_BATCH_LAST;
        }*/

        const mutation_update& update = mu->data.updates[i];
        dsn_message_t req = mu->client_requests[i];
        if (update.code != RPC_REPLICATION_WRITE_EMPTY)
        {
            dinfo("%s: mutation %s dispatch rpc call: %s",
                  _replica->name(), mu->name(), update.code.to_string());
         
            if (req == nullptr)
            {                
                req = dsn_msg_create_received_request(update.code, (void*)update.data.data(), update.data.length());
                dsn_layer1_app_commit_rpc_request(_app_context, req, true);
                dsn_msg_release_ref(req);
                req = nullptr;
            }
            else
            {
                dsn_layer1_app_commit_rpc_request(_app_context, req, true);
            }

            //binary_reader reader(update.data);
            //dsn_message_t resp = (req ? dsn_msg_create_response(req) : nullptr);

            ////uint64_t now = dsn_now_ns();
            //dispatch_rpc_call(update.code, reader, resp);
            ////now = dsn_now_ns() - now;

            ////_app_commit_latency.set(now);
        }
        else
        {
            // empty mutation write
        }

        int perr = dsn_layer1_app_get_physical_error(_app_context);
        if (perr != 0)
        {
            derror("%s: physical error %d occurs in replication local app %s",
                   _replica->name(), perr, data_dir().c_str());
            return ERR_LOCAL_APP_FAILURE;
        }
    }

    ++_last_committed_decree;
    //++_app_info->info.type1.last_committed_decree;

    _replica->update_commit_statistics(count);
    _app_commit_throughput.add(1);
    _app_req_throughput.add((uint64_t)count);
    _app_commit_decree.increment();

    return ERR_OK;
}

error_code replication_app_base::update_init_info(replica* r, int64_t shared_log_offset, int64_t private_log_offset)
{
    _info.crc = 0;
    _info.magic = 0xdeadbeef;
    _info.init_ballot = r->get_ballot();
    _info.init_decree = r->last_committed_decree();
    _info.init_offset_in_shared_log = shared_log_offset;
    _info.init_offset_in_private_log = private_log_offset;

    std::string info_path = utils::filesystem::path_combine(r->dir(), ".info");
    return _info.store(info_path.c_str());
}

}} // end namespace
