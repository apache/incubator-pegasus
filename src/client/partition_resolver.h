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

#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "common/gpid.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/task/async_calls.h"

namespace dsn {
namespace replication {

class partition_resolver : public ref_counter
{
public:
    static dsn::ref_ptr<partition_resolver>
    get_resolver(const char *cluster_name,
                 const std::vector<dsn::rpc_address> &meta_list,
                 const char *app_name);

    template <typename TReq, typename TCallback>
    dsn::rpc_response_task_ptr call_op(dsn::task_code code,
                                       TReq &&request,
                                       dsn::task_tracker *tracker,
                                       TCallback &&callback,
                                       std::chrono::milliseconds timeout,
                                       uint64_t partition_hash,
                                       int reply_hash = 0)
    {
        dsn::message_ex *msg = dsn::message_ex::create_request(
            code, static_cast<int>(timeout.count()), 0, partition_hash);
        marshall(msg, std::forward<TReq>(request));
        dsn::rpc_response_task_ptr response_task = rpc::create_rpc_response_task(
            msg, tracker, std::forward<TCallback>(callback), reply_hash);
        call_task(response_task);
        return response_task;
    }

    // choosing a proper replica server from meta server or local route cache
    // and send the read/write request.
    // if got reply or error, call the callback.
    // parameters like request data, timeout, callback handler are all wrapped
    // into "task", you may want to refer to dsn::rpc_response_task for details.
    void call_task(const dsn::rpc_response_task_ptr &task);

    std::string get_app_name() const { return _app_name; }

    dsn::rpc_address get_meta_server() const { return _meta_server; }

    const char *log_prefix() const { return _app_name.c_str(); }

protected:
    partition_resolver(rpc_address meta_server, const char *app_name)
        : _app_name(app_name), _meta_server(meta_server)
    {
    }

    virtual ~partition_resolver() {}

    struct resolve_result
    {
        ///< ERR_OK
        ///< ERR_SERVICE_NOT_FOUND if resolver or app is missing
        ///< ERR_IO_PENDING if resolve in is progress, callers
        ///< should call resolve_async in this case
        error_code err;
        ///< IPv4 of the target to send request to
        rpc_address address;
        ///< global partition indentity
        dsn::gpid pid;
    };

    /**
     * resolve partition_hash into IP or group addresses to know what to connect next
     *
     * \param partition_hash the partition hash
     * \param callback       callback invoked on completion or timeout
     * \param timeout_ms     timeout to execute the callback
     *
     * \return see \ref resolve_result for details
     */
    virtual void resolve(uint64_t partition_hash,
                         std::function<void(resolve_result &&)> &&callback,
                         int timeout_ms) = 0;

    /*!
     failure handler when access failed for certain partition

     \param partition_index zero-based index of the partition.
     \param err             error code

     this is usually to trigger new round of address resolve
     */
    virtual void on_access_failure(int partition_index, error_code err) = 0;

    /**
     * get zero-based partition index
     *
     * \param partition_count number of partitions.
     * \param partition_hash  the partition hash.
     *
     * \return zero-based partition index.
     */

    virtual int get_partition_index(int partition_count, uint64_t partition_hash) = 0;

    std::string _cluster_name;
    std::string _app_name;
    rpc_address _meta_server;
};

typedef ref_ptr<partition_resolver> partition_resolver_ptr;

} // namespace replication
} // namespace dsn
