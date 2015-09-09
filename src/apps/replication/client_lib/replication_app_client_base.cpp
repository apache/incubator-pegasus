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
#include "replication_common.h"
#include "rpc_replicated.h"

namespace dsn { namespace replication {

using namespace ::dsn::service;

void replication_app_client_base::load_meta_servers(
        /*out*/ std::vector<::dsn::rpc_address>& servers
        )
{
    // read meta_servers from machine list file
    servers.clear();

    const char* server_ss[10];
    int capacity = 10, need_count;
    need_count = dsn_config_get_all_keys("replication.meta_servers", server_ss, &capacity);
    dassert(need_count <= capacity, "too many meta servers specified");

    for (int i = 0; i < capacity; i++)
    {
        std::string s(server_ss[i]);

        // name:port
        auto pos1 = s.find_first_of(':');
        if (pos1 != std::string::npos)
        {
            ::dsn::rpc_address ep(HOST_TYPE_IPV4, s.substr(0, pos1).c_str(), atoi(s.substr(pos1 + 1).c_str()));
            servers.push_back(ep);
        }
    }
}

replication_app_client_base::replication_app_client_base(
    const std::vector<::dsn::rpc_address>& meta_servers, 
    const char* app_name,
    int task_bucket_count/* = 13*/
    )
    : clientlet(task_bucket_count)
{
    _app_name = std::string(app_name);   
    _meta_servers = meta_servers;

    _app_id = -1;
    _app_partition_count = -1;
    _last_contact_point.set_invalid();
}

replication_app_client_base::replication_app_client_base(
    const std::vector<::dsn::rpc_address>& meta_servers,
    const char* app_name,
    const char* host_app_type,
    int host_app_index,
    int task_bucket_count/* = 13*/
    )
    : clientlet(host_app_type, host_app_index, task_bucket_count)
{
    _app_name = std::string(app_name);
    _meta_servers = meta_servers;

    _app_id = -1;
    _app_partition_count = -1;
    _last_contact_point.set_invalid();
}

replication_app_client_base::~replication_app_client_base()
{
    clear_all_pending_tasks();
}

void replication_app_client_base::clear_all_pending_tasks()
{
    dsn_message_t nil(nullptr);

    service::zauto_lock l(_requests_lock);
    for (auto& pc : _pending_requests)
    {
        if (pc.second->query_config_task != nullptr)
            pc.second->query_config_task->cancel(true);

        for (auto& rc : pc.second->requests)
        {
            end_request(rc, ERR_TIMEOUT, nil);
        }
        delete pc.second;
    }
    _pending_requests.clear();
}

DEFINE_TASK_CODE(LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_REPLICATION_DELAY_QUERY_CONFIG, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

replication_app_client_base::request_context* replication_app_client_base::create_write_context(
    int partition_index,
    dsn_task_code_t code,
    dsn_message_t request,
    ::dsn::task_ptr& callback,
    int reply_hash
    )
{
    int timeout_milliseconds;
    dsn_msg_query_request(request, &timeout_milliseconds, nullptr);

    auto rc = new request_context;
    rc->request = request;
    rc->callback_task = callback;    
    rc->is_read = false;
    rc->partition_index = partition_index;    
    rc->write_header.gpid.app_id = _app_id;
    rc->write_header.gpid.pidx = partition_index;
    rc->write_header.code = code;
    rc->timeout_timer = nullptr;
    rc->timeout_ms = timeout_milliseconds;
    rc->timeout_ts_us = now_us() + timeout_milliseconds * 1000;
    rc->completed = false;

    size_t offset = dsn_msg_body_size(request);
    ::marshall(request, rc->write_header);

    if (rc->write_header.gpid.app_id == -1)
    {
        rc->header_pos = (char*)dsn_msg_rw_ptr(request, offset);
    }
    else
    {
        rc->header_pos = 0;
        dsn_msg_update_request(request, 0, gpid_to_hash(rc->write_header.gpid));
    }

    return rc;
}

replication_app_client_base::request_context* replication_app_client_base::create_read_context(
    int partition_index,
    dsn_task_code_t code,
    dsn_message_t request,
    ::dsn::task_ptr& callback,
    read_semantic_t read_semantic,
    decree snapshot_decree, // only used when ReadSnapshot        
    int reply_hash
    )
{
    int timeout_milliseconds;
    dsn_msg_query_request(request, &timeout_milliseconds, nullptr);

    auto rc = new request_context;
    rc->request = request;
    rc->callback_task = callback;    
    rc->is_read = true;
    rc->partition_index = partition_index;
    rc->read_header.gpid.app_id = _app_id;
    rc->read_header.gpid.pidx = partition_index;
    rc->read_header.code = code;
    rc->read_header.semantic = read_semantic;
    rc->read_header.version_decree = snapshot_decree;
    rc->timeout_timer = nullptr;
    rc->timeout_ms = timeout_milliseconds;
    rc->timeout_ts_us = now_us() + timeout_milliseconds * 1000;
    rc->completed = false;

    size_t offset = dsn_msg_body_size(request);
    ::marshall(request, rc->read_header);

    if (rc->read_header.gpid.app_id == -1)
    {
        rc->header_pos = (char*)dsn_msg_rw_ptr(request, offset);
    }
    else
    {
        rc->header_pos = 0;
        dsn_msg_update_request(request, 0 , gpid_to_hash(rc->read_header.gpid));
    }

    return rc;
}

void replication_app_client_base::on_user_request_timeout(request_context_ptr& rc)
{
    dsn_message_t nil(nullptr);
    end_request(rc, ERR_TIMEOUT, nil);
}

void replication_app_client_base::end_request(request_context_ptr& request, error_code err, dsn_message_t resp)
{
    zauto_lock l(request->lock);
    if (request->completed)
    {
        err.end_tracking();
        return;
    }

    if (err != ERR_TIMEOUT && request->timeout_timer != nullptr)
        request->timeout_timer->cancel(false);

    request->callback_task->enqueue_rpc_response(err, resp);
    request->completed = true;
}

void replication_app_client_base::call(request_context_ptr request, bool no_delay)
{
    if (!no_delay)
    {
        zauto_lock l(request->lock);
        if (request->completed)
            return;
    }

    auto nts = ::dsn_now_us();    
    if (nts + 100 >= request->timeout_ts_us) // within 100 us
    {
        dsn_message_t nil(nullptr);
        end_request(request, ERR_TIMEOUT, nil);
        return;
    }
 
    auto& msg = request->request;
    int timeout_ms;
    if (nts + 1000 > request->timeout_ts_us)
        timeout_ms = 1;
    else
        timeout_ms = static_cast<int>(request->timeout_ts_us - nts) / 1000;

    ::dsn::rpc_address addr;
    int app_id;

    error_code err = get_address(
        request->partition_index,
        !request->is_read,
        addr,
        app_id,
        request->read_header.semantic
        );

    // target node in cache
    if (err == ERR_OK)
    {
        dbg_dassert(addr.is_invalid() == false, "");
        dassert(app_id > 0, "");

        if (request->header_pos != 0)
        {
            if (request->is_read)
            {
                request->read_header.gpid.app_id = app_id;
                blob buffer(request->header_pos, 0, sizeof(request->read_header));
                binary_writer writer(buffer);
                marshall(writer, request->read_header);

                dsn_msg_update_request(request->request, timeout_ms, gpid_to_hash(request->read_header.gpid));
            }
            else
            {
                request->write_header.gpid.app_id = app_id;
                blob buffer(request->header_pos, 0, sizeof(request->write_header));
                binary_writer writer(buffer);
                marshall(writer, request->write_header);
                dsn_msg_update_request(request->request, timeout_ms, gpid_to_hash(request->write_header.gpid));
            }

            request->header_pos = 0;
        }
        else
        {
            dsn_msg_update_request(request->request, timeout_ms, DSN_INVALID_HASH);
        }

        {
            zauto_lock l(request->lock);
            rpc::call(
                addr,
                msg,
                this,
                std::bind(
                &replication_app_client_base::replica_rw_reply,
                this,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3,
                request
                )
                );
        }
    }

    // target node not known
    else if (!no_delay)
    {
        // delay 1 second for further config query
        // TODO: better policies here
        tasking::enqueue(LPC_REPLICATION_DELAY_QUERY_CONFIG, this,
            std::bind(&replication_app_client_base::call, this, request, true),
            0,
            1000
            );
    }
    
    else
    {
        {
            zauto_lock l(request->lock);

            // init timeout timer if necessary
            if (request->timeout_timer == nullptr)
            {
                request->timeout_timer = tasking::enqueue(
                    LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT,
                    this,
                    std::bind(&replication_app_client_base::on_user_request_timeout, this, request),
                    0,
                    timeout_ms
                    );
            }
        }

        zauto_lock l(_requests_lock);
        // put into pending queue of querying target partition 
        auto it = _pending_requests.find(request->partition_index);
        if (it == _pending_requests.end())
        {
            auto pc = new partition_context;
            pc->query_config_task = nullptr;
            it = _pending_requests.insert(pending_requests::value_type(request->partition_index, pc)).first;
        }

        it->second->requests.push_back(request);

        // init configuration query task if necessary
        if (it->second->query_config_task == nullptr)
        {
            dsn_message_t msg = dsn_msg_create_request(RPC_CM_CALL, 0, 0);

            meta_request_header hdr;
            hdr.rpc_tag = RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX;
            ::marshall(msg, hdr);

            configuration_query_by_index_request req;
            req.app_name = _app_name;
            req.partition_indices.push_back(request->partition_index);
            ::marshall(msg, req);
            
            it->second->query_config_task = rpc::call_replicated(
                _last_contact_point,
                _meta_servers,
                msg,

                this,
                std::bind(&replication_app_client_base::query_partition_configuration_reply,
                    this, 
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3,
                    request->partition_index
                    )
                );
        }
    }
}

void replication_app_client_base::replica_rw_reply(
    error_code err,
    dsn_message_t request,
    dsn_message_t response,
    request_context_ptr& rc
    )
{
    if (err != ERR_OK)
    {
        goto Retry;
    }

    ::unmarshall(response, err);
    
    if (err != ERR_OK && err != ERR_HANDLER_NOT_FOUND)
    {
        goto Retry;
    }
    else
    {
        end_request(rc, err, response);
    }
    return;

Retry:
    // clear partition configuration as it could be wrong
    {
        zauto_write_lock l(_config_lock);
        _config_cache.erase(rc->is_read ? rc->read_header.gpid.pidx : rc->write_header.gpid.pidx);
    }

    // then retry
    call(rc.get(), false);
}

error_code replication_app_client_base::get_address(int pidx, bool is_write, /*out*/ ::dsn::rpc_address& addr, /*out*/ int& app_id, read_semantic_t semantic)
{
    error_code err;
    partition_configuration config;
     
    {
    zauto_read_lock l(_config_lock);
    auto it = _config_cache.find(pidx);
    if (it != _config_cache.end())
    {
        err = ERR_OK;
        config = it->second;
    }
    else
    {
        err = ERR_IO_PENDING;
    }
    }

    if (err == ERR_OK)
    {
        app_id = _app_id;
        if (is_write)
        {
            addr = config.primary;
        }
        else
        {
            addr = get_read_address(semantic, config);
        }

        if (addr.is_invalid())
        {
            err = ERR_IO_PENDING;
        }
    } 
    return err;
}

void replication_app_client_base::query_partition_configuration_reply(error_code err, dsn_message_t request, dsn_message_t response, int pidx)
{
    if (err == ERR_OK)
    {
        configuration_query_by_index_response resp;
        ::unmarshall(response, resp);
        if (resp.err == ERR_OK)
        {
            zauto_write_lock l(_config_lock);
            dsn_msg_from_address(response, _last_contact_point.c_addr_ptr());

            if (resp.partitions.size() > 0)
            {
                if (_app_id != -1 && _app_id != resp.partitions[0].gpid.app_id)
                {
                    dassert(false, "app id is changed (mostly the app was removed and created with the same name), local Vs remote: %u vs %u ",
                        _app_id, resp.partitions[0].gpid.app_id);
                }

                _app_id = resp.partitions[0].gpid.app_id;
                _app_partition_count = resp.partition_count;
            }

            for (auto it = resp.partitions.begin(); it != resp.partitions.end(); it++)
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
            }
        }
    }
        
    // send pending client msgs
    partition_context* pc = nullptr;
    {
        zauto_lock l(_requests_lock);
        auto it = _pending_requests.find(pidx);
        if (it != _pending_requests.end())
        {
            pc = it->second;
            _pending_requests.erase(pidx);
        }
    }

    if (pc != nullptr)
    {
        for (auto& req : pc->requests)
        {   
            call(req.get(), false);
        }
        pc->requests.clear();
        delete pc;
    }
}

::dsn::rpc_address replication_app_client_base::get_read_address(read_semantic_t semantic, const partition_configuration& config)
{
    if (semantic == read_semantic_t::ReadLastUpdate)
        return config.primary;

    // readsnapshot or readoutdated, using random
    else
    {
        bool has_primary = false;
        int N = static_cast<int>(config.secondaries.size());
        if (config.primary.is_invalid() == false)
        {
            N++;
            has_primary = true;
        }

        if (0 == N) return config.primary;

        int r = random32(0, 1000) % N;
        if (has_primary && r == N - 1)
            return config.primary;
        else
            return config.secondaries[r];
    }
}

}} // end namespace
