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
 *     interface for clients operations using rDSN
 *
 * Revision history:
 *     Dec., 2015, @xiaotz (Xiaotong Zhang), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */


#pragma once

//
// replication_client_base is the base class for clients to access
// all app to be replicated using this library
//
# include <dsn/service_api_cpp.h>
# include <dsn/dist/replication/replication.types.h>
# include <dsn/dist/replication/replication_other_types.h>
# include <dsn/dist/replication/replication.codes.h>
# include <dsn/dist/replication/replication_app_client_base.h>

using namespace dsn::replication;
using namespace dsn;

namespace dsn { namespace replication {

#pragma pack(push, 4)
class replication_client_base : public virtual clientlet
{
public:
    replication_client_base(
        const std::vector<dsn::rpc_address>& meta_servers
        );
    ~replication_client_base();

    template<typename TRequest, typename TResponse>
    ::dsn::task_ptr request_meta(
        const char* app_name,
        dsn_task_code_t code,
        std::shared_ptr<TRequest>& req,
        // callback
        clientlet* owner,
        std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,

        // other specific parameters
        int timeout_milliseconds = 0,
        int reply_hash = 0
        )
    {
        replication_app_client_base* client_base = get_app_client_base(app_name);
        dassert(client_base != nullptr, "");
        return client_base->request_meta(code, req, owner, callback, timeout_milliseconds, reply_hash);
    }

    template<typename TRequest, typename TResponse>
    ::dsn::task_ptr write(
        const char* app_name,
        std::function<int(int)> get_partition_index,
        dsn_task_code_t code,
        std::shared_ptr<TRequest>& req,
        // callback
        clientlet* owner,
        std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,
        // other specific parameters
        int timeout_milliseconds = 0,
        int reply_hash = 0
        )
    {
        replication_app_client_base* client_base = get_app_client_base(app_name);
        dassert(client_base != nullptr, "");
        return client_base->write(get_partition_index, code, req, owner, callback, timeout_milliseconds, reply_hash);
    }

    template<typename TRequest, typename TResponse>
    ::dsn::task_ptr read(
        const char* app_name,
        std::function<int(int)> get_partition_index,
        dsn_task_code_t code,
        std::shared_ptr<TRequest>& req,

        // callback
        clientlet* owner,
        std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,

        // other specific parameters
        int timeout_milliseconds = 0,
        int reply_hash = 0,
        ::dsn::replication::read_semantic_t read_semantic = ReadOutdated,
        decree snapshot_decree = invalid_decree // only used when ReadSnapshot
        )
    {
        replication_app_client_base* client_base = get_app_client_base(app_name);
        dassert(client_base != nullptr, "");
        return client_base->read(get_partition_index, code, req, owner, callback, timeout_milliseconds, reply_hash, read_semantic, snapshot_decree);
    }

private:
    replication_app_client_base* get_app_client_base(const char* app_name);

private:
    mutable ::dsn::service::zrwlock_nr _config_lock;
    std::vector<dsn::rpc_address> _meta_servers;
    std::unordered_map<std::string, replication_app_client_base*> _app_clients;
};
#pragma pack(pop)
}} // namespace
