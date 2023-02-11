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

#include "client/partition_resolver.h"

// IWYU pragma: no_include <type_traits>

#include "partition_resolver_manager.h"
#include "runtime/api_layer1.h"
#include "runtime/api_task.h"
#include "runtime/task/task_spec.h"
#include "utils/fmt_logging.h"
#include "utils/threadpool_code.h"

namespace dsn {
namespace replication {
/*static*/
partition_resolver_ptr partition_resolver::get_resolver(const char *cluster_name,
                                                        const std::vector<rpc_address> &meta_list,
                                                        const char *app_name)
{
    return partition_resolver_manager::instance().find_or_create(cluster_name, meta_list, app_name);
}

DEFINE_TASK_CODE(LPC_RPC_DELAY_CALL, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

static inline bool error_retry(error_code err)
{
    return (err != ERR_HANDLER_NOT_FOUND && err != ERR_APP_NOT_EXIST &&
            err != ERR_OPERATION_DISABLED && err != ERR_BUSY && err != ERR_SPLITTING &&
            err != ERR_DISK_INSUFFICIENT);
}

void partition_resolver::call_task(const rpc_response_task_ptr &t)
{
    auto &hdr = *(t->get_request()->header);
    uint64_t deadline_ms = dsn_now_ms() + hdr.client.timeout_ms;

    rpc_response_handler old_callback;
    t->fetch_current_handler(old_callback);
    auto new_callback = [ this, deadline_ms, oc = std::move(old_callback) ](
        dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (req->header->gpid.value() != 0 && err != ERR_OK && error_retry(err)) {
            on_access_failure(req->header->gpid.get_partition_index(), err);
            // still got time, retry
            uint64_t nms = dsn_now_ms();
            uint64_t gap = 8 << req->send_retry_count;
            if (gap > 1000)
                gap = 1000;
            if (nms + gap < deadline_ms) {
                req->send_retry_count++;
                req->header->client.timeout_ms = static_cast<int>(deadline_ms - nms - gap);

                rpc_response_task_ptr ctask =
                    dynamic_cast<rpc_response_task *>(task::get_current_task());
                partition_resolver_ptr r(this);

                CHECK_NOTNULL(ctask, "current task must be rpc_response_task");
                ctask->replace_callback(std::move(oc));
                CHECK(ctask->set_retry(false),
                      "rpc_response_task set retry failed, state = {}",
                      enum_to_string(ctask->state()));

                // sleep gap milliseconds before retry
                tasking::enqueue(LPC_RPC_DELAY_CALL,
                                 nullptr,
                                 [r, ctask]() { r->call_task(ctask); },
                                 0,
                                 std::chrono::milliseconds(gap));
                return;
            } else {
                LOG_ERROR("service access failed ({}), no more time for further tries, set error "
                          "= ERR_TIMEOUT, trace_id = {:#018x}",
                          err,
                          req->header->trace_id);
                err = ERR_TIMEOUT;
            }
        }

        if (oc)
            oc(err, req, resp);
    };
    t->replace_callback(std::move(new_callback));

    resolve(hdr.client.partition_hash,
            [t](resolve_result &&result) mutable {
                if (result.err != ERR_OK) {
                    t->enqueue(result.err, nullptr);
                    return;
                }

                // update gpid when necessary
                auto &hdr = *(t->get_request()->header);
                if (hdr.gpid.value() != result.pid.value()) {
                    if (hdr.client.thread_hash == 0 // thread_hash is not assigned by applications
                        ||
                        hdr.gpid.value() != 0 // requests set to child redirect to parent
                        ) {
                        hdr.client.thread_hash = result.pid.thread_hash();
                    }
                    hdr.gpid = result.pid;
                }
                dsn_rpc_call(result.address, t.get());
            },
            hdr.client.timeout_ms);
}
} // namespace replication
} // namespace dsn
