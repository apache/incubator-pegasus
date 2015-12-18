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

#include "replication_common.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replication.app.client.base.cpp"

namespace dsn { namespace replication {

using namespace ::dsn::service;

/*static*/void replication_app_client_base::load_meta_servers(
    /*out*/ std::vector<dsn::rpc_address>& servers,
    const char* section /*= "replication.meta_servers"*/)
{
    // read meta_servers from machine list file
    servers.clear();

    const char* server_ss[10];
    int capacity = 10, need_count;
    need_count = dsn_config_get_all_keys(section, server_ss, &capacity);
    dassert(need_count <= capacity, "too many meta servers specified in config [%s]", section);

    for (int i = 0; i < capacity; i++)
    {
        std::string s(server_ss[i]);

        // name:port
        auto pos1 = s.find_first_of(':');
        if (pos1 != std::string::npos)
        {
            ::dsn::rpc_address ep(s.substr(0, pos1).c_str(), atoi(s.substr(pos1 + 1).c_str()));
            servers.push_back(ep);
        }
    }
    dassert(servers.size() > 0, "no meta server specified in config [%s]", section);
}

replication_app_client_base::replication_app_client_base(
    const std::vector< ::dsn::rpc_address>& meta_servers, 
    const char* app_name,
    int task_bucket_count/* = 13*/
    )
    : clientlet(task_bucket_count),
      _app_id(-1), _app_partition_count(-1),_app_name(app_name)
{
    _meta_servers.assign_group(dsn_group_build("meta.servers"));
    for (auto& m : meta_servers)
        dsn_group_add(_meta_servers.group_handle(), m.c_addr());

}

replication_app_client_base::~replication_app_client_base()
{
    clear_all_pending_tasks();
    dsn_group_destroy(_meta_servers.group_handle());
}

void replication_app_client_base::clear_all_pending_tasks()
{
    dinfo("clear all pending tasks");
    dsn_message_t nil(nullptr);
    service::zauto_lock l(_requests_lock);
    //clear _pending_replica_requests
    for (auto& pc : _pending_replica_requests)
    {
        if (pc.second->query_config_task != nullptr)
            pc.second->query_config_task->cancel(true);

        for (auto& rc : pc.second->requests)
        {
            end_request(rc, ERR_TIMEOUT, nil);
        }
        delete pc.second;
    }
    _pending_replica_requests.clear();

    //clear _pending_meta_requests
    _pending_meta_requests.query_config_task->cancel(true);
    _pending_meta_requests.query_config_task = nullptr;
    for (auto& rc : _pending_meta_requests.requests)
    {
        end_request(rc, ERR_TIMEOUT, nil);
    }
    _pending_meta_requests.requests.clear();
}

DEFINE_TASK_CODE(LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_REPLICATION_DELAY_QUERY_CONFIG, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

replication_app_client_base::request_context* replication_app_client_base::create_write_context(
    uint64_t key_hash,
    dsn_task_code_t code,
    dsn_message_t request,
    ::dsn::task_ptr& callback,
    int reply_hash /*= 0*/
    )
{
    int timeout_milliseconds;
    dsn_msg_query_request(request, &timeout_milliseconds, nullptr);

    auto rc = new request_context;
    rc->request = request;
    rc->callback_task = callback;
    rc->is_read = false;
    rc->partition_index = -1;
    rc->key_hash = key_hash;
    rc->write_header.gpid.app_id = _app_id;
    rc->write_header.gpid.pidx = -1;
    rc->write_header.code = dsn_task_code_to_string(code);
    rc->timeout_timer = nullptr;
    rc->timeout_ms = timeout_milliseconds;
    rc->timeout_ts_us = now_us() + timeout_milliseconds * 1000;
    rc->completed = false;

    size_t offset = dsn_msg_body_size(request);
    ::marshall(request, rc->write_header);

    if (rc->write_header.gpid.pidx == -1)
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
    uint64_t key_hash,
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
    rc->partition_index = -1;
    rc->key_hash = key_hash;
    rc->read_header.gpid.app_id = _app_id;
    rc->read_header.gpid.pidx = -1;
    rc->read_header.code = dsn_task_code_to_string(code);
    rc->read_header.semantic = read_semantic;
    rc->read_header.version_decree = snapshot_decree;
    rc->timeout_timer = nullptr;
    rc->timeout_ms = timeout_milliseconds;
    rc->timeout_ts_us = now_us() + timeout_milliseconds * 1000;
    rc->completed = false;

    size_t offset = dsn_msg_body_size(request);
    ::marshall(request, rc->read_header);

    if (rc->read_header.gpid.pidx == -1)
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

void replication_app_client_base::on_replica_request_timeout(request_context_ptr& rc)
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
    int timeout_ms;
    if (nts + 1000 > request->timeout_ts_us)
        timeout_ms = 1;
    else
        timeout_ms = static_cast<int>(request->timeout_ts_us - nts) / 1000;
    {
        zauto_lock l(request->lock);
        // init timeout timer if necessary
        if (request->timeout_timer == nullptr)
        {
            dinfo("set timeout timer");
            request->timeout_timer = tasking::enqueue(
                LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT,
                this,
                std::bind(&replication_app_client_base::on_replica_request_timeout, this, request),
                0,
                timeout_ms
                );
        }
    }

    // not found app_name
    if(_app_id == -1)
    {
        dinfo("not have app id, query for app id first, app_name:%s", _app_name.c_str());
        dbg_dassert(_app_partition_count == -1, "");
        // target node not known
        if (!no_delay)
        {
            dinfo("call delay");
            // delay 1 second for further config query
            // TODO: better policies here
            tasking::enqueue(LPC_REPLICATION_DELAY_QUERY_CONFIG, this,
                std::bind(&replication_app_client_base::call, this, request, true),
                0,
                1000
                );
            return;
        }
        else
        {
            dinfo("call no delay");
            zauto_lock l(_requests_lock);
            _pending_meta_requests.requests.push_back(request);
            if (_pending_meta_requests.query_config_task == nullptr)
            {
                _pending_meta_requests.query_config_task = query_partition_config(request);
            }
            return;
        }
    }
    request->partition_index = get_partition_index(_app_partition_count, request->key_hash);
    get_address_and_call(request, no_delay);
}

void replication_app_client_base::get_address_and_call(
        request_context_ptr request,
        bool no_delay)
{
    dinfo("get partition index:%d", request->partition_index);
    dbg_dassert(request->partition_index != -1, "");

    ::dsn::rpc_address addr;
    error_code err = get_address(request->partition_index, !request->is_read, addr, request->read_header.semantic);
    if(err == ERR_OK)
    {
        call_with_address(addr, request, no_delay);
        return;
    }
    // target node not known
    else if (!no_delay)
    {
        dinfo("get_address_and_call delay");
        // delay 1 second for further config query
        // TODO: better policies here
        tasking::enqueue(LPC_REPLICATION_DELAY_QUERY_CONFIG, this,
            std::bind(&replication_app_client_base::get_address_and_call, this, request, true),
            0,
            1000
            );
    }
    //didn't get address and no_delay
    else
    {
        dinfo("get_address_and_call no delay");

        zauto_lock l(_requests_lock);
        // put into pending queue of querying target partition
        auto it = _pending_replica_requests.find(request->partition_index);
        if (it == _pending_replica_requests.end())
        {
            auto pc = new partition_context;
            pc->query_config_task = nullptr;
            it = _pending_replica_requests.insert(pending_replica_requests::value_type(request->partition_index, pc)).first;
        }

        it->second->requests.push_back(request);
        // init configuration query task if necessary
        if (it->second->query_config_task == nullptr)
        {
            it->second->query_config_task = query_partition_config(request);
        }
    }
}

void replication_app_client_base::call_with_address(dsn::rpc_address addr, request_context_ptr request, bool no_delay)
{
    auto& msg = request->request;

    dbg_dassert(!addr.is_invalid(), "");
    dbg_dassert(_app_id > 0, "");

    if (request->header_pos != 0)
    {
        if (request->is_read)
        {
            request->read_header.gpid.app_id = _app_id;
            request->read_header.gpid.pidx = request->partition_index;
            blob buffer(request->header_pos, 0, sizeof(request->read_header));
            binary_writer writer(buffer);
            marshall(writer, request->read_header);

            dsn_msg_update_request(request->request, request->timeout_ms, gpid_to_hash(request->read_header.gpid));
        }
        else
        {
            request->write_header.gpid.app_id = _app_id;
            request->write_header.gpid.pidx = request->partition_index;
            blob buffer(request->header_pos, 0, sizeof(request->write_header));
            binary_writer writer(buffer);
            marshall(writer, request->write_header);
            dsn_msg_update_request(request->request, request->timeout_ms, gpid_to_hash(request->write_header.gpid));
        }
        request->header_pos = 0;
    }

    {
        dinfo("call_with_address[%s] rpc call, gpid:[%s,%d:%d]", addr.to_string(), _app_name.c_str(), _app_id, request->partition_index);
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

void replication_app_client_base::on_meta_request_timeout(meta_context_ptr& rc)
{
    dsn_message_t nil(nullptr);
    end_meta_request(rc, ERR_TIMEOUT, nil);
}

void replication_app_client_base::end_meta_request(meta_context_ptr& request, error_code err, dsn_message_t resp)
{
    zauto_lock l(request->lock);
    if (request->completed)
    {
        dinfo("already time out before meta reply");
        err.end_tracking();
        return;
    }

    if (err != ERR_TIMEOUT && request->timeout_timer != nullptr)
    {
        request->timeout_timer->cancel(false);
    }
    request->callback_task->enqueue_rpc_response(err, resp);
    request->completed = true;
}

void replication_app_client_base::call_meta(meta_context_ptr request)
{
    dinfo("start call_meta");
    {
        zauto_lock l(request->lock);
        if (request->completed)
            return;
    }

    auto nts = ::dsn_now_us();
    int timeout_ms;
    if (nts + 1000 > request->timeout_ts_us)
        timeout_ms = 1;
    else
        timeout_ms = static_cast<int>(request->timeout_ts_us - nts) / 1000;
    {
        zauto_lock l(request->lock);
        // init timeout timer if necessary
        if (request->timeout_timer == nullptr)
        {
            dinfo("set timeout timer");
            request->timeout_timer = tasking::enqueue(
                LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT,
                this,
                std::bind(&replication_app_client_base::on_meta_request_timeout, this, request),
                0,
                timeout_ms
                );
        }
    }

    rpc_address target(_meta_servers);
    rpc::call(
        target,
        request->request,
        this,
        std::bind(&replication_app_client_base::meta_rw_reply,
            this,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3,
            request
            ),
            0
        );
}


/*callback*/
void replication_app_client_base::replica_rw_reply(
    error_code err,
    dsn_message_t request,
    dsn_message_t response,
    request_context_ptr& rc
    )
{
    {
        zauto_lock l(rc->lock);
        if (rc->completed)
        {
            dinfo("already time out before replica reply");
            err.end_tracking();
            return;
        }
    }

    if (err != ERR_OK)
    {
        goto Retry;
    }

    ::unmarshall(response, err);

    // TODO: zhangxiaotong
    // decide which err from replica should retry.
    if (err != ERR_OK && err != ERR_HANDLER_NOT_FOUND)
    {
        dsn::rpc_address adr = dsn_msg_from_address(response);
        dinfo("replica_rw_reply response, err = %s, addr[%s]", err.to_string(), adr.to_string());
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
        dwarn("met error[%d:%s], erase partition config cache, %d", err.get(), err.to_string(), rc->partition_index);
        _config_cache.erase(rc->partition_index);
    }
    // then retry
    call(rc.get(), false);
}

void replication_app_client_base::meta_rw_reply(
    error_code err,
    dsn_message_t request,
    dsn_message_t response,
    meta_context_ptr& rc
    )
{
    dinfo("meta_rw_reply, err=[%d,%s]", err.get(), err.to_string());
    /*if (err == ERR_OK || err == ERR_BUSY_CREATING)
    {
        zauto_write_lock l(_config_lock);
    }*/
    if (err != ERR_OK && err != ERR_TIMEOUT)
    {
        goto Retry;
    }
    end_meta_request(rc, err, response);
Retry:

    dinfo("call delay");
    // delay 1 second for further config query
    // TODO: better policies here
    tasking::enqueue(LPC_REPLICATION_DELAY_QUERY_CONFIG, this,
        std::bind(&replication_app_client_base::call_meta, this, rc),
        0,
        1000
        );
    return;
}

void replication_app_client_base::query_partition_configuration_reply(error_code err, dsn_message_t request, dsn_message_t response, int pidx)
{
    dinfo("query_partition_configuration_reply, gpid=[%s,%d,%d], err=%s", _app_name.c_str(), _app_id, pidx, dsn_error_to_string(err));

    bool conti = true;
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
            if ( _app_partition_count != -1 && _app_partition_count != resp.partition_count)
            {
                dassert(false, "partition count is changed (mostly the app was removed and created with the same name), local Vs remote: %u vs %u ",
                        _app_partition_count, resp.partition_count);
            }
            _app_id = resp.app_id;
            _app_partition_count = resp.partition_count;

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
                else if (it2->second.ballot > new_config.ballot)
                {
                    dwarn("config from metas older than client, gpid[%s,%d:%d], ballot [%u vs %u]",_app_name.c_str(), _app_id, pidx, it2->second.ballot, new_config.ballot);
                }
            }
        }
        else if(resp.err == ERR_OBJECT_NOT_FOUND)
        {
            derror("can't find table on meta server, stop and return client error code");
            conti = false;
            client_err = ERR_APP_NOT_EXIST;
        }
        else
        {
            derror("met unknown error, gpid[%s,%d:%d], error[%d:%s]", _app_name.c_str(), _app_id, pidx, resp.err.get(), resp.err.to_string());
            conti = false;
            client_err = resp.err;
        }
    }
    else
    {
        derror("did not get meta response, network issue, gpid[%s,%d:%d], error[%d:%s]", _app_name.c_str(), _app_id, pidx, err.get(), err.to_string());
    }

    // get address call
    if(pidx != -1)
    {
        // send pending client msgs
        partition_context* pc = nullptr;
        {
            zauto_lock l(_requests_lock);
            auto it = _pending_replica_requests.find(pidx);
            if (it != _pending_replica_requests.end())
            {
                pc = it->second;
                _pending_replica_requests.erase(pidx);
            }
        }

        if (pc != nullptr)
        {
            for (auto& req : pc->requests)
            {
                if(conti)
                {
                    get_address_and_call(req, false);
                }
                else
                {
                    dsn_message_t nil(nullptr);
                    end_request(req, client_err, nil);
                }
            }
            pc->requests.clear();
            delete pc;
        }
    }
    else // just get app info
    {
        std::list<request_context_ptr> tmp;
        {
            zauto_lock l(_requests_lock);
            _pending_meta_requests.query_config_task = nullptr;
            tmp = _pending_meta_requests.requests;
            _pending_meta_requests.requests.clear();
        }
        for (auto& req : tmp)
        {
            if(conti)
            {
                call(req, false);
            }
            else
            {
                dsn_message_t nil(nullptr);
                end_request(req, client_err, nil);
            }
        }
        tmp.clear();
    }
}

/*send rpc*/
dsn::task_ptr replication_app_client_base::query_partition_config(request_context_ptr request)
{
    dinfo("query_partition_config, gpid:[%s,%d,%d]", _app_name.c_str(), _app_id, request->partition_index);
    dsn_message_t msg = dsn_msg_create_request(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, 0, 0);

    configuration_query_by_index_request req;
    req.app_name = _app_name;
    if(request->partition_index != -1)
    {
        req.partition_indices.push_back(request->partition_index);
    }
    ::marshall(msg, req);

    rpc_address target(_meta_servers);
    return rpc::call(
        target,
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

/*search in cache*/
dsn::rpc_address replication_app_client_base::get_address(bool is_write, read_semantic_t semantic, const partition_configuration& config)
{
    if (is_write || semantic == read_semantic_t::ReadLastUpdate)
        return config.primary;

    // readsnapshot or readoutdated, using random
    else
    {
        bool has_primary = false;
        int N = static_cast<int>(config.secondaries.size());
        if (!config.primary.is_invalid())
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

//ERR_OBJECT_NOT_FOUND  not in cache.
//ERR_IO_PENDING        in cache but invalid, remove from cache.
//ERR_OK                in cache and valid
error_code replication_app_client_base::get_address(int pidx, bool is_write, /*out*/ dsn::rpc_address& addr, read_semantic_t semantic)
{
    error_code err = ERR_OBJECT_NOT_FOUND;

    partition_configuration config;
    {
        zauto_read_lock l(_config_lock);
        auto it = _config_cache.find(pidx);
        if(it != _config_cache.end())
        {
            dinfo("get_address found address");
            config = it->second;
            err = ERR_OK;
        }
    }

    if (err == ERR_OK)
    {
        addr = get_address(is_write, semantic, config);
        if (addr.is_invalid())
        {
            zauto_write_lock l(_config_lock);
            _config_cache.erase(pidx);
            derror("cache have address but invalid gpid:[%s,%d:%d]", _app_name.c_str(), _app_id, pidx);
            err = ERR_IO_PENDING;
        }
        else
        {
            dinfo("get_address addr[%s]", addr.to_string());
        }
    }
    else
    {
        ddebug("not find address in cache for gpid:[%s,%d]", _app_name.c_str(), pidx);
    }
    return err;
}

int replication_app_client_base::get_partition_index(int partition_count, uint64_t key_hash)
{
    return key_hash % (uint64_t)partition_count;
}

}} // end namespace
