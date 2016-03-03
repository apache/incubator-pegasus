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
*     partition resolver simple provider implementation
*
* Revision history:
*     Feb., 2016, @imzhenyu (Zhenyu Guo), first draft
*     xxxx-xx-xx, author, fix bug about xxx
*/

# include "partition_resolver_simple.h"
# include <dsn/cpp/utils.h>

namespace dsn
{
    namespace dist
    {
        //------------------------------------------------------------------------------------
        using namespace ::dsn::service;
        using namespace ::dsn::replication;

        partition_resolver_simple::partition_resolver_simple(
            rpc_address meta_server,
            const char* app_path
            )
            : partition_resolver(meta_server, app_path),
            _app_id(-1), _app_partition_count(-1), _app_is_stateful(true)
        {
        }

        void partition_resolver_simple::resolve(
            uint64_t partition_hash,
            std::function<void(dist::partition_resolver::resolve_result&&)>&& callback,
            int timeout_ms
            )
        {
            int idx = -1;
            if (_app_partition_count != -1)
            {
                idx = get_partition_index(_app_partition_count, partition_hash);
                rpc_address target;
                if (ERR_OK == get_address(idx, target))
                {
                    callback(partition_resolver::resolve_result{
                        ERR_OK,
                        target,
                        {_app_id, idx}
                    });
                    return;
                }
            }
            
            auto rc = new request_context();
            rc->partition_hash = partition_hash;
            rc->callback = std::move(callback);
            rc->partition_index = idx;
            rc->timeout_timer = nullptr;
            rc->timeout_ms = timeout_ms;
            rc->timeout_ts_us = now_us() + timeout_ms * 1000;
            rc->completed = false;

            call(std::move(rc), false);
        }

        void partition_resolver_simple::on_access_failure(int partition_index, error_code err)
        {
            if (-1 != partition_index)
            {
                ddebug("clear partition configuration cache %d.%d due to access failure %s",
                    _app_id, partition_index, err.to_string());

                {
                    zauto_read_lock l(_config_lock);
                    _config_cache.erase(partition_index);
                }
            }
        }

        partition_resolver_simple::~partition_resolver_simple()
        {
            clear_all_pending_requests();
            dsn_group_destroy(_meta_server.group_handle());
        }

        void partition_resolver_simple::clear_all_pending_requests()
        {
            dinfo("%s.client: clear all pending tasks", _app_path.c_str());
            service::zauto_lock l(_requests_lock);
            //clear _pending_requests
            for (auto& pc : _pending_requests)
            {
                if (pc.second->query_config_task != nullptr)
                    pc.second->query_config_task->cancel(true);

                for (auto& rc : pc.second->requests)
                {
                    end_request(std::move(rc), ERR_TIMEOUT, rpc_address());
                }
                delete pc.second;
            }
            _pending_requests.clear();
        }

        DEFINE_TASK_CODE(LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
        DEFINE_TASK_CODE(LPC_REPLICATION_DELAY_QUERY_CONFIG, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
            
        void partition_resolver_simple::on_timeout(request_context_ptr&& rc)
        {
            dsn_message_t nil(nullptr);
            end_request(std::move(rc), ERR_TIMEOUT, rpc_address());
        }

        void partition_resolver_simple::end_request(request_context_ptr&& request, error_code err, rpc_address addr)
        {
            zauto_lock l(request->lock);
            if (request->completed)
            {
                err.end_tracking();
                return;
            }

            if (err != ERR_TIMEOUT && request->timeout_timer != nullptr)
                request->timeout_timer->cancel(false);

            request->callback(partition_resolver::resolve_result{
                err, 
                addr, 
                {_app_id, request->partition_index}
            });
            request->completed = true;
        }

        void partition_resolver_simple::call(request_context_ptr&& request, bool from_meta_ack)
        {
            int pindex = request->partition_index;
            if (-1 != pindex)
            {
                // fill target address if possible
                ::dsn::rpc_address addr;
                error_code err = get_address(pindex, addr);

                // target address known
                if (err == ERR_OK)
                {
                    end_request(std::move(request), ERR_OK, addr);
                    return;
                }
            }
            
            auto nts = ::dsn_now_us();

            // timeout will happen very soon, no way to get the rpc call done
            if (nts + 100 >= request->timeout_ts_us) // within 100 us
            {
                dsn_message_t nil(nullptr);
                end_request(std::move(request), ERR_TIMEOUT, rpc_address());
                return;
            }

            // calculate timeout
            int timeout_ms;
            if (nts + 1000 > request->timeout_ts_us)
                timeout_ms = 1;
            else
                timeout_ms = static_cast<int>(request->timeout_ts_us - nts) / 1000;
            
            // init timeout timer only when necessary
            {
                zauto_lock l(request->lock);
                if (request->timeout_timer == nullptr)
                {
                    request->timeout_timer = tasking::enqueue(
                        LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT,
                        this,
                        [=, req2 = request]() mutable
                        {
                            on_timeout(std::move(req2));
                        },
                        0,
                        std::chrono::milliseconds(timeout_ms)
                        );
                }
            }

            {
                zauto_lock l(_requests_lock);
                if (-1 != pindex)
                {
                    // put into pending queue of querying target partition
                    auto it = _pending_requests.find(pindex);
                    if (it == _pending_requests.end())
                    {
                        auto pc = new partition_context;
                        pc->query_config_task = nullptr;
                        it = _pending_requests.emplace(pindex, pc).first;
                    }
                    it->second->requests.push_back(std::move(request));

                    // init configuration query task if necessary
                    if (nullptr == it->second->query_config_task)
                    {
                        // TODO: delay if from_meta_ack = true
                        it->second->query_config_task = query_partition_config(pindex);
                    }
                }
                else
                {
                    _pending_requests_before_partition_count_unknown.push_back(std::move(request));
                    if (_pending_requests_before_partition_count_unknown.size() == 1)
                    {
                        _query_config_task = query_partition_config(pindex);
                    }
                }
            }
        }
        
        ///*callback*/
        //void partition_resolver_simple::replica_rw_reply(
        //    error_code err,
        //    dsn_message_t request,
        //    dsn_message_t response,
        //    request_context_ptr rc
        //    )
        //{
        //    {
        //        zauto_lock l(rc->lock);
        //        if (rc->completed)
        //        {
        //            //dinfo("already time out before replica reply");
        //            err.end_tracking();
        //            return;
        //        }
        //    }

        //    if (err != ERR_OK)
        //    {
        //        goto Retry;
        //    }

        //    ::unmarshall(response, err);

        //    //
        //    // some error codes do not need retry
        //    //
        //    if (err == ERR_OK || err == ERR_HANDLER_NOT_FOUND)
        //    {
        //        end_request(rc, err, response);
        //        return;
        //    }

        //    // retry 
        //    else
        //    {
        //        dsn::rpc_address adr = dsn_msg_from_address(response);
        //    }

        //Retry:
        //    dinfo("%s.client: get error %s from replica with index %d",
        //        _app_path.c_str(),
        //        err.to_string(),
        //        rc->partition_index
        //        );

        //    // clear partition configuration as it could be wrong
        //    {
        //        zauto_write_lock l(_config_lock);
        //        _config_cache.erase(rc->partition_index);
        //    }

        //    // then retry
        //    call(rc.get(), false);
        //}


        /*send rpc*/
        dsn::task_ptr partition_resolver_simple::query_partition_config(int partition_index)
        {
            //dinfo("query_partition_config, gpid:[%s,%d,%d]", _app_path.c_str(), _app_id, request->partition_index);
            dsn_message_t msg = dsn_msg_create_request(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, 0, 0);

            configuration_query_by_index_request req;
            req.app_name = _app_path;
            if (partition_index != -1)
            {
                req.partition_indices.push_back(partition_index);
            }
            ::marshall(msg, req);

            rpc_address target(_meta_server);
            return rpc::call(
                target,
                msg,
                this,
                [this, partition_index](error_code err, dsn_message_t req, dsn_message_t resp)
                {
                    query_partition_configuration_reply(err, req, resp, partition_index);
                }
                );
        }

        void partition_resolver_simple::query_partition_configuration_reply(error_code err, dsn_message_t request, dsn_message_t response, int partition_index)
        {
            error_code client_err = ERR_OK;

            if (err == ERR_OK)
            {
                configuration_query_by_index_response resp;
                ::unmarshall(response, resp);
                if (resp.err == ERR_OK)
                {
                    zauto_write_lock l(_config_lock);

                    if (_app_id != -1 && _app_id != resp.app_id)
                    {
                        dassert(false, "app id is changed (mostly the app was removed and created with the same name), local Vs remote: %u vs %u ",
                            _app_id, resp.app_id);
                    }
                    if (_app_partition_count != -1 && _app_partition_count != resp.partition_count)
                    {
                        dassert(false, "partition count is changed (mostly the app was removed and created with the same name), local Vs remote: %u vs %u ",
                            _app_partition_count, resp.partition_count);
                    }
                    _app_id = resp.app_id;
                    _app_partition_count = resp.partition_count;
                    _app_is_stateful = resp.is_stateful;

                    for (auto it = resp.partitions.begin(); it != resp.partitions.end(); ++it)
                    {
                        partition_configuration& new_config = *it;

                        auto it2 = _config_cache.find(new_config.gpid.pidx);
                        if (it2 == _config_cache.end())
                        {
                            _config_cache[new_config.gpid.pidx] = new_config;
                        }
                        else if (it2->second.ballot < new_config.ballot)
                        {
                            it2->second = new_config;
                        }
                        else
                        {
                            // nothing to do
                        }
                    }
                }
                else if (resp.err == ERR_OBJECT_NOT_FOUND)
                {
                    derror("%s.client: query config reply err = %s, partition index = %d",
                        _app_path.c_str(),
                        resp.err.to_string(),
                        partition_index
                        );

                    client_err = ERR_APP_NOT_EXIST;
                }
                else
                {
                    derror("%s.client: query config reply err = %s, partition index = %d",
                        _app_path.c_str(),
                        resp.err.to_string(),
                        partition_index
                        );

                    client_err = resp.err;
                }
            }
            else
            {
                derror("%s.client: query config reply err = %s, partition index = %d",
                    _app_path.c_str(),
                    err.to_string(),
                    partition_index
                    );
            }

            // get specific or all partition update
            if (partition_index != -1)
            {
                partition_context* pc = nullptr;
                {
                    zauto_lock l(_requests_lock);
                    auto it = _pending_requests.find(partition_index);
                    if (it != _pending_requests.end())
                    {
                        pc = it->second;
                        _pending_requests.erase(partition_index);
                    }
                }

                if (pc)
                {
                    handle_pending_requests(pc->requests, client_err);
                    delete pc;
                }
            }

            // get all partition update
            else
            {
                pending_replica_requests reqs;
                std::list<request_context_ptr> reqs2;
                {
                    zauto_lock l(_requests_lock);
                    reqs = std::move(_pending_requests);
                    _pending_requests.clear();

                    reqs2 = std::move(_pending_requests_before_partition_count_unknown);
                    _pending_requests_before_partition_count_unknown.clear();
                }
             
                if (reqs2.size() > 0)
                {
                    for (auto& req : reqs2)
                    {
                        dassert(-1 == req->partition_index, "");
                        req->partition_index = get_partition_index(_app_partition_count, req->partition_hash);
                    }
                    handle_pending_requests(reqs2, client_err);
                }

                for (auto& r : reqs)
                {
                    if (r.second)
                    {
                        handle_pending_requests(r.second->requests, client_err);
                        delete r.second;
                    }
                }
                reqs.clear();
            }
        }

        void partition_resolver_simple::handle_pending_requests(std::list<request_context_ptr>& reqs, error_code err)
        {
            for (auto& req : reqs)
            {
                if (err == ERR_OK)
                {
                    rpc_address addr;
                    err = get_address(req->partition_index, addr);
                    if (err == ERR_OK)
                    {
                        end_request(std::move(req), err, addr);
                    }
                    else
                    {
                        call(std::move(req), true);
                    }
                }
                else if (err == ERR_HANDLER_NOT_FOUND)
                {
                    end_request(std::move(req), err, rpc_address());
                }
                else
                {
                    call(std::move(req), true);
                }
            }
            reqs.clear();
        }

        /*search in cache*/
        dsn::rpc_address partition_resolver_simple::get_address(const partition_configuration& config)
        {
            if (_app_is_stateful)
                return config.primary;
            else
            {
                if (config.last_drops.size() == 0)
                {
                    return ::dsn::rpc_address();
                }
                else
                {
                    return config.last_drops[dsn_random32(0, config.last_drops.size() - 1)];
                }
            }

            //if (is_write || semantic == read_semantic::ReadLastUpdate)
            //    return config.primary;

            //// readsnapshot or readoutdated, using random
            //else
            //{
            //    bool has_primary = false;
            //    int N = static_cast<int>(config.secondaries.size());
            //    if (!config.primary.is_invalid())
            //    {
            //        N++;
            //        has_primary = true;
            //    }

            //    if (0 == N) return config.primary;

            //    int r = random32(0, 1000) % N;
            //    if (has_primary && r == N - 1)
            //        return config.primary;
            //    else
            //        return config.secondaries[r];
            //}
        }

        //ERR_OBJECT_NOT_FOUND  not in cache.
        //ERR_IO_PENDING        in cache but invalid, remove from cache.
        //ERR_OK                in cache and valid
        error_code partition_resolver_simple::get_address(int pidx, /*out*/ dsn::rpc_address& addr)
        {
            partition_configuration config;
            {
                zauto_read_lock l(_config_lock);
                auto it = _config_cache.find(pidx);
                if (it != _config_cache.end())
                {
                    config = it->second;
                    addr = get_address(config);
                    if (addr.is_invalid())
                    {
                        return ERR_IO_PENDING;
                    }
                    else
                    {
                        return ERR_OK;
                    }
                }
                else
                {
                    return ERR_OBJECT_NOT_FOUND;
                }
            }
        }

        int partition_resolver_simple::get_partition_index(int partition_count, uint64_t partition_hash)
        {
            return partition_hash % (uint64_t)partition_count;
        }
    }
}