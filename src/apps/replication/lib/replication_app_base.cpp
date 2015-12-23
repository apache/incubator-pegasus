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

void register_replica_provider(replica_app_factory f, const char* name)
{
    ::dsn::utils::factory_store<replication_app_base>::register_factory(name, f, PROVIDER_TYPE_MAIN);
}

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

    dinfo("update app init info in %s, ballot = %" PRId64 ", decree = %" PRId64 ", log_offset<S,P> = <%" PRId64 ",%" PRId64 ">",
        ffile.c_str(),
        init_ballot,
        init_decree,
        init_offset_in_shared_log,
        init_offset_in_private_log
        );
    return ERR_OK;
}

replication_app_base::replication_app_base(replica* replica)
{
    _physical_error = 0;
    _dir_data = replica->dir() + "/data";
    _dir_learn = replica->dir() + "/learn";
    _is_delta_state_learning_supported = false;
    _batch_state = BS_NOT_BATCH;

    _replica = replica;
    _last_committed_decree = _last_durable_decree = 0;

    if (!dsn::utils::filesystem::create_directory(_dir_data))
    {
        dassert(false, "Fail to create directory %s.", _dir_data.c_str());
    }

    if (!dsn::utils::filesystem::create_directory(_dir_learn))
    {
        dassert(false, "Fail to create directory %s.", _dir_learn.c_str());
    }

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
    _app_commit_throughput.init(ss.str().c_str(), COUNTER_TYPE_RATE, "commit throughput for current app");

    ss.clear();
    ss << replica_name() << ".latency(ns)";
    _app_commit_latency.init(ss.str().c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, "commit latency for current app");

    ss.clear();
    ss << replica_name() << ".decree#";
    _app_commit_decree.init(ss.str().c_str(), COUNTER_TYPE_NUMBER, "commit decree for current app");
}

error_code replication_app_base::open_internal(replica* r, bool create_new)
{
    auto err = open(create_new);
    if (err == ERR_OK)
    {
        dassert(last_committed_decree() == last_durable_decree(), "");
        if (!create_new)
        {
            std::string info_path = utils::filesystem::path_combine(r->dir(), ".info");
            err = _info.load(info_path.c_str());
        }
    }

    _app_commit_decree.add(last_committed_decree());

    return err;
}

error_code replication_app_base::write_internal(mutation_ptr& mu)
{
    dassert (mu->data.header.decree == last_committed_decree() + 1, "");
    dassert(mu->client_requests.size() == mu->data.updates.size()
        && mu->client_requests.size() > 0, 
        "data inconsistency in mutation");

    int count = static_cast<int>(mu->client_requests.size());
    _batch_state = (count == 1 ? BS_NOT_BATCH : BS_BATCH);
    for (int i = 0; i < count; i++)
    {
        if (_batch_state == BS_BATCH && i + 1 == count)
        {
            _batch_state = BS_BATCH_LAST;
        }

        auto& r = mu->client_requests[i];
        if (r.code != RPC_REPLICATION_WRITE_EMPTY)
        {
            dinfo("%s: mutation %s dispatch rpc call: %s",
                  _replica->name(), mu->name(), dsn_task_code_to_string(r.code));
            binary_reader reader(mu->data.updates[i]);
            dsn_message_t resp = (r.req ? dsn_msg_create_response(r.req) : nullptr);

            uint64_t now = dsn_now_ns();
            dispatch_rpc_call(r.code, reader, resp);
            now = dsn_now_ns() - now;

            _app_commit_latency.set(now);
        }
        else
        {
            // empty mutation write
        }

        if (_physical_error != 0)
        {
            derror("%s: physical error %d occurs in replication local app %s",
                   _replica->name(), _physical_error, data_dir().c_str());
            return ERR_LOCAL_APP_FAILURE;
        }
    }

    ++_last_committed_decree;

    _replica->update_commit_statistics(count);
    _app_commit_throughput.add((uint64_t)count);
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

void replication_app_base::dispatch_rpc_call(int code, binary_reader& reader, dsn_message_t response)
{
    auto it = _handlers.find(code);
    if (it != _handlers.end())
    {
        if (response)
        {
            int err = 0; // replication layer error
            ::marshall(response, err);            
        }
        it->second(reader, response);
    }
    else
    {
        dassert(false, "cannot find handler for rpc code %d in %s", code, data_dir().c_str());
    }
}

}} // end namespace
