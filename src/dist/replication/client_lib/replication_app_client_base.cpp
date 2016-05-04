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
    /*out*/ std::vector<dsn::rpc_address>& servers)
{
    servers.clear();
    std::string server_list = dsn_config_get_value_string("meta_server", "server_list", "", "meta server list");
    std::list<std::string> lv;
    ::dsn::utils::split_args(server_list.c_str(), lv, ',');
    for (auto& s : lv)
    {
        // name:port
        auto pos1 = s.find_first_of(':');
        if (pos1 != std::string::npos)
        {
            ::dsn::rpc_address ep(s.substr(0, pos1).c_str(), atoi(s.substr(pos1 + 1).c_str()));
            servers.push_back(ep);
        }
    }
    dassert(servers.size() > 0, "no meta server specified in config [meta_server].server_list");
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
    dinfo("%s.client: clear all pending tasks", _app_name.c_str());
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
}

DEFINE_TASK_CODE(LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_REPLICATION_DELAY_QUERY_CONFIG, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_REPLICATION_DELAY_QUERY_CALL, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

replication_app_client_base::request_context* replication_app_client_base::create_write_context(
    uint64_t key_hash,
    dsn_task_code_t code,
    dsn_message_t request,
    ::dsn::task_ptr& callback,
    int reply_hash /*= 0*/
    )
{
    dsn_msg_options_t opts;
    dsn_msg_get_options(request, &opts);

    auto rc = new request_context;
    rc->request = request;
    rc->callback_task = callback;
    rc->is_read = false;
    rc->partition_index = -1;
    rc->key_hash = key_hash;
    rc->write_header.gpid.app_id = _app_id;
    rc->write_header.gpid.pidx = -1;
    rc->write_header.code = task_code(code);
    rc->timeout_timer = nullptr;
    rc->timeout_ms = opts.timeout_ms;
    rc->timeout_ts_us = now_us() + opts.timeout_ms * 1000;
    rc->completed = false;

    size_t offset = dsn_msg_body_size(request);
    ::marshall(request, rc->write_header);
    rc->header_pos = (char*)dsn_msg_rw_ptr(request, offset);

    return rc;
}

replication_app_client_base::request_context* replication_app_client_base::create_read_context(
    uint64_t key_hash,
    dsn_task_code_t code,
    dsn_message_t request,
    ::dsn::task_ptr& callback,
    read_semantic semantic,
    decree snapshot_decree, // only used when ReadSnapshot
    int reply_hash
    )
{
    dsn_msg_options_t opts;
    dsn_msg_get_options(request, &opts);

    auto rc = new request_context;
    rc->request = request;
    rc->callback_task = callback;
    rc->is_read = true;
    rc->partition_index = -1;
    rc->key_hash = key_hash;
    rc->read_header.gpid.app_id = _app_id;
    rc->read_header.gpid.pidx = -1;
    rc->read_header.code = task_code(code);
    rc->read_header.semantic = semantic;
    rc->read_header.version_decree = snapshot_decree;
    rc->timeout_timer = nullptr;
    rc->timeout_ms = opts.timeout_ms;
    rc->timeout_ts_us = now_us() + opts.timeout_ms * 1000;
    rc->completed = false;

    size_t offset = dsn_msg_body_size(request);
    ::marshall(request, rc->read_header);
    rc->header_pos = (char*)dsn_msg_rw_ptr(request, offset);

    return rc;
}

void replication_app_client_base::on_replica_request_timeout(request_context_ptr& rc)
{
    dsn_message_t nil(nullptr);
    end_request(rc, ERR_TIMEOUT, nil, true);
}

void replication_app_client_base::end_request(request_context_ptr& request, error_code err, dsn_message_t resp, bool called_by_timer)
{
    zauto_lock l(request->lock);
    if (request->completed)
    {
        err.end_tracking();
        return;
    }

    if (request->timeout_timer != nullptr && !called_by_timer)
        request->timeout_timer->cancel(false);

    request->callback_task->enqueue_rpc_response(err, resp);
    request->completed = true;
}

void replication_app_client_base::call(request_context_ptr request, bool from_meta_ack)
{
    if (request->timeout_timer != nullptr)
    {
        // maybe already completed by timeout
        zauto_lock l(request->lock);
        if (request->completed)
            return;
    }

    auto nts = ::dsn_now_us();

    // timeout will happen very soon, no way to get the rpc call done
    if (nts + 100 >= request->timeout_ts_us) // within 100 us
    {
        dsn_message_t nil(nullptr);
        end_request(request, ERR_TIMEOUT, nil);
        return;
    }

    // calculate timeout
    int timeout_ms;
    if (nts + 1000 > request->timeout_ts_us)
        timeout_ms = 1;
    else
        timeout_ms = static_cast<int>(request->timeout_ts_us - nts) / 1000;

    // fill partition index
    if (_app_partition_count == -1)
    {
        // init timeout timer only when necessary
        // DO NOT START THE TIMER FOR EACH REQUEST
        {
            zauto_lock l(request->lock);
            if (request->timeout_timer == nullptr)
            {
                request->timeout_timer = tasking::enqueue(
                    LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT,
                    this,
                    std::bind(&replication_app_client_base::on_replica_request_timeout, this, request),
                    0,
                    std::chrono::milliseconds(timeout_ms)
                    );
            }
        }

        if (from_meta_ack)
        {
            // delay 1 second for further config query
            tasking::enqueue(LPC_REPLICATION_DELAY_QUERY_CONFIG, this,
                std::bind(&replication_app_client_base::call, this, request, false),
                0,
                std::chrono::seconds(1)
                );
        }
        else
        {
            zauto_lock l(_requests_lock);
            query_config(request);
        }

        return;
    }

    // calculate partition index
    if (request->partition_index == -1)
    {
        request->partition_index = get_partition_index(_app_partition_count, request->key_hash);
    }
    
    // fill target address
    ::dsn::rpc_address addr;
    error_code err = get_address(request->partition_index, !request->is_read, addr, request->read_header.semantic);

    // target address known
    if (err == ERR_OK)
    {
        call_with_address(addr, request);
    }

    // target node not known
    else
    {
        if (from_meta_ack)
        {
            // delay 1 second for further config query
            tasking::enqueue(LPC_REPLICATION_DELAY_QUERY_CONFIG, this,
                std::bind(&replication_app_client_base::call, this, request, false),
                0,
                std::chrono::seconds(1)
                );
        }

        else
        {
            // init timeout timer only when necessary
            // DO NOT START THE TIMER FOR EACH REQUEST
            {
                zauto_lock l(request->lock);
                if (request->timeout_timer == nullptr)
                {
                    request->timeout_timer = tasking::enqueue(
                        LPC_REPLICATION_CLIENT_REQUEST_TIMEOUT,
                        this,
                        std::bind(&replication_app_client_base::on_replica_request_timeout, this, request),
                        0,
                        std::chrono::milliseconds(timeout_ms)
                        );
                }
            }

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
                it->second->query_config_task = query_config(request);
            }
        }
    }
}

void replication_app_client_base::call_with_address(dsn::rpc_address addr, request_context_ptr request)
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

            dsn_msg_options_t opts;
            opts.timeout_ms = request->timeout_ms;
            opts.request_hash = gpid_to_hash(request->read_header.gpid);
            opts.vnid = *(uint64_t*)(&request->read_header.gpid);
            dsn_msg_set_options(request->request, &opts, DSN_MSGM_HASH | DSN_MSGM_TIMEOUT); // TODO: not supported yet DSN_MSGM_VNID);
        }
        else
        {
            request->write_header.gpid.app_id = _app_id;
            request->write_header.gpid.pidx = request->partition_index;
            blob buffer(request->header_pos, 0, sizeof(request->write_header));
            binary_writer writer(buffer);
            marshall(writer, request->write_header);

            dsn_msg_options_t opts;
            opts.timeout_ms = request->timeout_ms;
            opts.request_hash = gpid_to_hash(request->write_header.gpid);
            opts.vnid = *(uint64_t*)(&request->write_header.gpid);
            
            dsn_msg_set_options(request->request, &opts, DSN_MSGM_HASH | DSN_MSGM_TIMEOUT); // TODO: not supported yet DSN_MSGM_VNID | DSN_MSGM_CONTEXT);
        }
        request->header_pos = 0;
    }

    {
        zauto_lock l(request->lock);
        rpc::call(
            addr,
            msg,
            this,
            [this, request](error_code err, dsn_message_t req, dsn_message_t resp)
            {
                replication_app_client_base::replica_rw_reply(err, req, resp, request);
            }
        );
    }
}

/*callback*/
void replication_app_client_base::replica_rw_reply(
    error_code err,
    dsn_message_t request,
    dsn_message_t response,
    request_context_ptr rc
    )
{
    {
        zauto_lock l(rc->lock);
        if (rc->completed)
        {
            //dinfo("already time out before replica reply");
            err.end_tracking();
            return;
        }
    }

    bool clear_config_cache = false;
    bool retry = false;

    if (err != ERR_OK)
    {
        // network issue
        clear_config_cache = true;
        retry = true;
        goto Handle_Failure;
    }

    ::unmarshall(response, err);

    //
    // some error codes do not need retry
    //

    // server is busy, retry later
    // server can't serve, retry later
    if(err == ERR_CAPACITY_EXCEEDED || err == ERR_NOT_ENOUGH_MEMBER)
    {
        clear_config_cache = false;
        retry = true;
    }
    // the sever state is not primary
    // the replica is not on the server
    else if(err == ERR_INVALID_STATE || err == ERR_OBJECT_NOT_FOUND)
    {
        clear_config_cache = true;
        retry = true;
    }
    // ERR_OK means success
    // the task code is not valid, version mismatch
    else if(err == ERR_OK || err == ERR_INVALID_DATA)
    {
        clear_config_cache = false;
        retry = false;
    }
    else
    {
        dassert(false, "%s.client: get error %s from replica with index %d, retry:%d, clear cache:%d",
            _app_name.c_str(),
            err.to_string(),
            rc->partition_index,
            retry,
            clear_config_cache
            );
    }

Handle_Failure:
    if(err != ERR_OK)
    {
        derror("%s.client: get error %s from replica with gpid=%d.%d, clear_config_cache=%s, retry=%s",
            _app_name.c_str(),
            err.to_string(),
            _app_id,
            rc->partition_index,
            clear_config_cache ? "true" : "false",
            retry ? "true" : "false"
            );
    }

    if(clear_config_cache)
    {
        zauto_write_lock l(_config_lock);
        _config_cache.erase(rc->partition_index);
    }

    if(!retry)
    {
        // no need to retry
        end_request(rc, err, (err == ERR_OK ? response : dsn_message_t(nullptr)));
        return;
    }

    else
    {
        // if no more than 1 second left, unnecessary to retry
        // because we need to sleep 1 second before retry
        auto nts = ::dsn_now_us();
        if (nts + 1000000 >= rc->timeout_ts_us)
        {
            dsn_message_t nil(nullptr);
            end_request(rc, ERR_TIMEOUT, nil);
            return;
        }

        // sleep 1 second before retry
        tasking::enqueue(
            LPC_REPLICATION_DELAY_QUERY_CALL,
            this,
            [this, rc_capture = std::move(rc)]()
            {
                replication_app_client_base::call(std::move(rc_capture));
            },
            0,
            std::chrono::seconds(1)
            );
    }
}

void replication_app_client_base::query_config_reply(error_code err, dsn_message_t request, dsn_message_t response, request_context_ptr context)
{    
    int pidx = context->partition_index;
    error_code client_err = ERR_OK;
    bool conti = true; // if can continue to the next step

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
                _app_name.c_str(),
                resp.err.to_string(),
                pidx
                );

            conti = false;
            client_err = ERR_APP_NOT_EXIST;
        }
        else
        {
            derror("%s.client: query config reply err = %s, partition index = %d",
                _app_name.c_str(),
                resp.err.to_string(),
                pidx
                );

            conti = false;
            client_err = resp.err;
        }
    }
    else
    {
        dwarn("%s.client: query config reply err = %s, partition index = %d",
            _app_name.c_str(),
            err.to_string(),
            pidx
            );
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
                    call(req, true);
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
        if(conti)
        {
            call(context, true);
        }
        else
        {
            dsn_message_t nil(nullptr);
            end_request(context, client_err, nil);
        }
    }
}

/*send rpc*/
dsn::task_ptr replication_app_client_base::query_config(request_context_ptr request)
{
    //dinfo("query_partition_config, gpid:[%s,%d,%d]", _app_name.c_str(), _app_id, request->partition_index);
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
        [this, request_capture = std::move(request)](error_code err, dsn_message_t req, dsn_message_t resp)
        {
            replication_app_client_base::query_config_reply(err, req, resp, std::move(request_capture));
        }
        );
}

/*search in cache*/
dsn::rpc_address replication_app_client_base::get_address(bool is_write, read_semantic semantic, const partition_configuration& config)
{
    if (is_write || semantic == read_semantic::ReadLastUpdate)
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
error_code replication_app_client_base::get_address(int pidx, bool is_write, /*out*/ dsn::rpc_address& addr, read_semantic semantic)
{
    partition_configuration config;
    {
        zauto_read_lock l(_config_lock);
        auto it = _config_cache.find(pidx);
        if(it != _config_cache.end())
        {
            config = it->second;
            addr = get_address(is_write, semantic, config);
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

int replication_app_client_base::get_partition_index(int partition_count, uint64_t key_hash)
{
    return key_hash % (uint64_t)partition_count;
}

}} // end namespace
