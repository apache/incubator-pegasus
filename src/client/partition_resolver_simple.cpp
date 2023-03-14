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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "partition_resolver_simple.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_spec.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/rand.h"
#include "utils/threadpool_code.h"

namespace dsn {
namespace replication {

partition_resolver_simple::partition_resolver_simple(rpc_address meta_server, const char *app_name)
    : partition_resolver(meta_server, app_name),
      _app_id(-1),
      _app_partition_count(-1),
      _app_is_stateful(true)
{
}

void partition_resolver_simple::resolve(uint64_t partition_hash,
                                        std::function<void(resolve_result &&)> &&callback,
                                        int timeout_ms)
{
    int idx = -1;
    if (_app_partition_count != -1) {
        idx = get_partition_index(_app_partition_count, partition_hash);
        rpc_address target;
        auto err = get_address(idx, target);
        if (dsn_unlikely(err == ERR_CHILD_NOT_READY)) {
            // child partition is not ready, its requests should be sent to parent partition
            idx -= _app_partition_count / 2;
            err = get_address(idx, target);
        }
        if (dsn_likely(err == ERR_OK)) {
            callback(resolve_result{ERR_OK, target, {_app_id, idx}});
            return;
        }
    }

    auto rc = new request_context();
    rc->partition_hash = partition_hash;
    rc->callback = std::move(callback);
    rc->partition_index = idx;
    rc->timeout_timer = nullptr;
    rc->timeout_ms = timeout_ms;
    rc->timeout_ts_us = dsn_now_us() + timeout_ms * 1000;
    rc->completed = false;

    call(std::move(rc), false);
}

void partition_resolver_simple::on_access_failure(int partition_index, error_code err)
{
    // ERR_CAPACITY_EXCEEDED : no need for reconfiguration on primary
    // ERR_NOT_ENOUGH_MEMBER : primary won't change and we only r/w on primary in this provider
    if (-1 == partition_index || err == ERR_CAPACITY_EXCEEDED || err == ERR_NOT_ENOUGH_MEMBER) {
        return;
    }

    zauto_write_lock l(_config_lock);
    if (err == ERR_PARENT_PARTITION_MISUSED) {
        LOG_INFO("clear all partition configuration cache due to access failure {} at {}.{}",
                 err,
                 _app_id,
                 partition_index);
        _app_partition_count = -1;
    } else {
        LOG_INFO("clear partition configuration cache {}.{} due to access failure {}",
                 _app_id,
                 partition_index,
                 err);
        _config_cache.erase(partition_index);
    }
}

partition_resolver_simple::~partition_resolver_simple()
{
    _tracker.cancel_outstanding_tasks();
    clear_all_pending_requests();
}

void partition_resolver_simple::clear_all_pending_requests()
{
    LOG_DEBUG_PREFIX("clear all pending tasks");
    zauto_lock l(_requests_lock);
    // clear _pending_requests
    for (auto &pc : _pending_requests) {
        if (pc.second->query_config_task != nullptr)
            pc.second->query_config_task->cancel(true);

        for (auto &rc : pc.second->requests) {
            end_request(std::move(rc), ERR_TIMEOUT, rpc_address());
        }
        delete pc.second;
    }
    _pending_requests.clear();
}

void partition_resolver_simple::on_timeout(request_context_ptr &&rc) const
{
    end_request(std::move(rc), ERR_TIMEOUT, rpc_address(), true);
}

void partition_resolver_simple::end_request(request_context_ptr &&request,
                                            error_code err,
                                            rpc_address addr,
                                            bool called_by_timer) const
{
    zauto_lock l(request->lock);
    if (request->completed) {
        return;
    }

    if (!called_by_timer && request->timeout_timer != nullptr)
        request->timeout_timer->cancel(false);

    request->callback(resolve_result{err, addr, {_app_id, request->partition_index}});
    request->completed = true;
}

DEFINE_TASK_CODE(LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_REPLICATION_DELAY_QUERY_CONFIG, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

void partition_resolver_simple::call(request_context_ptr &&request, bool from_meta_ack)
{
    int pindex = request->partition_index;
    if (-1 != pindex) {
        // fill target address if possible
        rpc_address addr;
        auto err = get_address(pindex, addr);

        // target address known
        if (err == ERR_OK) {
            end_request(std::move(request), ERR_OK, addr);
            return;
        }
    }

    auto nts = dsn_now_us();

    // timeout will happen very soon, no way to get the rpc call done
    if (nts + 100 >= request->timeout_ts_us) // within 100 us
    {
        end_request(std::move(request), ERR_TIMEOUT, rpc_address());
        return;
    }

    // delay 1 second for further config query
    if (from_meta_ack) {
        tasking::enqueue(LPC_REPLICATION_DELAY_QUERY_CONFIG,
                         &_tracker,
                         [ =, req2 = request ]() mutable { call(std::move(req2), false); },
                         0,
                         std::chrono::seconds(1));
        return;
    }

    // calculate timeout
    int timeout_ms;
    if (nts + 1000 >= request->timeout_ts_us)
        timeout_ms = 1;
    else
        timeout_ms = static_cast<int>(request->timeout_ts_us - nts) / 1000;

    // init timeout timer only when necessary
    {
        zauto_lock l(request->lock);
        if (request->timeout_timer == nullptr) {
            request->timeout_timer =
                tasking::enqueue(LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT,
                                 &_tracker,
                                 [ =, req2 = request ]() mutable { on_timeout(std::move(req2)); },
                                 0,
                                 std::chrono::milliseconds(timeout_ms));
        }
    }

    {
        zauto_lock l(_requests_lock);
        if (-1 != pindex) {
            // put into pending queue of querying target partition
            auto it = _pending_requests.find(pindex);
            if (it == _pending_requests.end()) {
                auto pc = new partition_context();
                it = _pending_requests.emplace(pindex, pc).first;
            }
            it->second->requests.push_back(std::move(request));

            // init configuration query task if necessary
            if (nullptr == it->second->query_config_task) {
                it->second->query_config_task = query_config(pindex, timeout_ms);
            }
        } else {
            _pending_requests_before_partition_count_unknown.push_back(std::move(request));
            if (_pending_requests_before_partition_count_unknown.size() == 1) {
                _query_config_task = query_config(pindex, timeout_ms);
            }
        }
    }
}

/*send rpc*/
DEFINE_TASK_CODE_RPC(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
                     TASK_PRIORITY_COMMON,
                     THREAD_POOL_DEFAULT)

task_ptr partition_resolver_simple::query_config(int partition_index, int timeout_ms)
{
    LOG_DEBUG_PREFIX(
        "start query config, gpid = {}.{}, timeout_ms = {}", _app_id, partition_index, timeout_ms);
    task_spec *sp = task_spec::get(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
    if (timeout_ms >= sp->rpc_timeout_milliseconds)
        timeout_ms = 0;
    auto msg = dsn::message_ex::create_request(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, timeout_ms);

    query_cfg_request req;
    req.app_name = _app_name;
    if (partition_index != -1) {
        req.partition_indices.push_back(partition_index);
    }
    marshall(msg, req);

    return rpc::call(
        _meta_server,
        msg,
        &_tracker,
        [this, partition_index](error_code err, dsn::message_ex *req, dsn::message_ex *resp) {
            query_config_reply(err, req, resp, partition_index);
        });
}

void partition_resolver_simple::query_config_reply(error_code err,
                                                   dsn::message_ex *request,
                                                   dsn::message_ex *response,
                                                   int partition_index)
{
    auto client_err = ERR_OK;

    if (err == ERR_OK) {
        query_cfg_response resp;
        unmarshall(response, resp);
        if (resp.err == ERR_OK) {
            zauto_write_lock l(_config_lock);

            if (_app_id != -1 && _app_id != resp.app_id) {
                LOG_WARNING(
                    "app id is changed (mostly the app was removed and created with the same "
                    "name), local vs remote: {} vs {} ",
                    _app_id,
                    resp.app_id);
            }
            if (_app_partition_count != -1 && _app_partition_count != resp.partition_count &&
                _app_partition_count * 2 != resp.partition_count &&
                _app_partition_count != resp.partition_count * 2) {
                LOG_WARNING("partition count is changed (mostly the app was removed and created "
                            "with the same name), local vs remote: {} vs {} ",
                            _app_partition_count,
                            resp.partition_count);
            }
            _app_id = resp.app_id;
            _app_partition_count = resp.partition_count;
            _app_is_stateful = resp.is_stateful;

            for (auto it = resp.partitions.begin(); it != resp.partitions.end(); ++it) {
                auto &new_config = *it;

                LOG_DEBUG_PREFIX("query config reply, gpid = {}, ballot = {}, primary = {}",
                                 new_config.pid,
                                 new_config.ballot,
                                 new_config.primary);

                auto it2 = _config_cache.find(new_config.pid.get_partition_index());
                if (it2 == _config_cache.end()) {
                    std::unique_ptr<partition_info> pi(new partition_info);
                    pi->timeout_count = 0;
                    pi->config = new_config;
                    _config_cache.emplace(new_config.pid.get_partition_index(), std::move(pi));
                } else if (_app_is_stateful && it2->second->config.ballot < new_config.ballot) {
                    it2->second->timeout_count = 0;
                    it2->second->config = new_config;
                } else if (!_app_is_stateful) {
                    it2->second->timeout_count = 0;
                    it2->second->config = new_config;
                } else {
                    // nothing to do
                }
            }
        } else if (resp.err == ERR_OBJECT_NOT_FOUND) {
            LOG_ERROR_PREFIX(
                "query config reply, gpid = {}.{}, err = {}", _app_id, partition_index, resp.err);

            client_err = ERR_APP_NOT_EXIST;
        } else {
            LOG_ERROR_PREFIX(
                "query config reply, gpid = {}.{}, err = {}", _app_id, partition_index, resp.err);

            client_err = resp.err;
        }
    } else {
        LOG_ERROR_PREFIX(
            "query config reply, gpid = {}.{}, err = {}", _app_id, partition_index, err);
    }

    // get specific or all partition update
    if (partition_index != -1) {
        partition_context *pc = nullptr;
        {
            zauto_lock l(_requests_lock);
            auto it = _pending_requests.find(partition_index);
            if (it != _pending_requests.end()) {
                pc = it->second;
                _pending_requests.erase(partition_index);
            }
        }

        if (pc) {
            handle_pending_requests(pc->requests, client_err);
            delete pc;
        }
    }

    // get all partition update
    else {
        pending_replica_requests reqs;
        std::deque<request_context_ptr> reqs2;
        {
            zauto_lock l(_requests_lock);
            reqs.swap(_pending_requests);
            reqs2.swap(_pending_requests_before_partition_count_unknown);
        }

        if (!reqs2.empty()) {
            if (_app_partition_count != -1) {
                for (auto &req : reqs2) {
                    CHECK_EQ(req->partition_index, -1);
                    req->partition_index =
                        get_partition_index(_app_partition_count, req->partition_hash);
                }
            }
            handle_pending_requests(reqs2, client_err);
        }

        for (auto &r : reqs) {
            if (r.second) {
                handle_pending_requests(r.second->requests, client_err);
                delete r.second;
            }
        }
    }
}

void partition_resolver_simple::handle_pending_requests(std::deque<request_context_ptr> &reqs,
                                                        error_code err)
{
    for (auto &req : reqs) {
        if (err == ERR_OK) {
            rpc_address addr;
            err = get_address(req->partition_index, addr);
            if (err == ERR_OK) {
                end_request(std::move(req), err, addr);
            } else {
                call(std::move(req), true);
            }
        } else if (err == ERR_HANDLER_NOT_FOUND || err == ERR_APP_NOT_EXIST ||
                   err == ERR_OPERATION_DISABLED) {
            end_request(std::move(req), err, rpc_address());
        } else {
            call(std::move(req), true);
        }
    }
    reqs.clear();
}

/*search in cache*/
rpc_address partition_resolver_simple::get_address(const partition_configuration &config) const
{
    if (_app_is_stateful) {
        return config.primary;
    } else {
        if (config.last_drops.size() == 0) {
            return rpc_address();
        } else {
            return config.last_drops[rand::next_u32(0, config.last_drops.size() - 1)];
        }
    }
}

error_code partition_resolver_simple::get_address(int partition_index, /*out*/ rpc_address &addr)
{
    // partition_configuration config;
    {
        zauto_read_lock l(_config_lock);
        auto it = _config_cache.find(partition_index);
        if (it != _config_cache.end()) {
            // config = it->second->config;
            if (it->second->config.ballot < 0) {
                // client query config for splitting app, child partition is not ready
                return ERR_CHILD_NOT_READY;
            }
            addr = get_address(it->second->config);
            if (addr.is_invalid()) {
                return ERR_IO_PENDING;
            } else {
                return ERR_OK;
            }
        } else {
            return ERR_OBJECT_NOT_FOUND;
        }
    }
}

int partition_resolver_simple::get_partition_index(int partition_count, uint64_t partition_hash)
{
    return partition_hash % static_cast<uint64_t>(partition_count);
}
} // namespace replication
} // namespace dsn
