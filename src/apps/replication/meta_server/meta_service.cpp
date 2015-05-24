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
#include "meta_service.h"
#include "server_state.h"
#include "load_balancer.h"
#include "meta_server_failure_detector.h"
#include <boost/filesystem.hpp>
#include <sys/stat.h>

meta_service::meta_service(server_state* state)
: _state(state), serverlet("meta_service")
{
    _balancer = nullptr;
    _failure_detector = nullptr;
    _log = static_cast<handle_t>(0);
    _offset = 0;
    _data_dir = ".";
    _started = false;

    _opts.initialize(system::config());
}

meta_service::~meta_service(void)
{
}

void meta_service::start(const char* data_dir, bool clean_state)
{
    _data_dir = data_dir;

    if (clean_state)
    {
        try {
            boost::filesystem::remove(_data_dir + "/checkpoint");
            boost::filesystem::remove(_data_dir + "/oplog");
        }
        catch (std::exception& ex)
        {
            ex;
        }
    }
    else
    {
        if (!boost::filesystem::exists(_data_dir))
        {
            boost::filesystem::create_directory(_data_dir);
        }

        if (boost::filesystem::exists(_data_dir + "/checkpoint"))
        {
            _state->load((_data_dir + "/checkpoint").c_str());
        }

        if (boost::filesystem::exists(_data_dir + "/oplog"))
        {
            replay_log((_data_dir + "/oplog").c_str());
            _state->save((_data_dir + "/checkpoint").c_str());
            boost::filesystem::remove(_data_dir + "/oplog");
        }
    }

    _log = file::open((_data_dir + "/oplog").c_str(), O_RDWR | O_CREAT, 0666);

    _balancer = new load_balancer(_state);            
    _failure_detector = new meta_server_failure_detector(_state);
    _balancer_timer = tasking::enqueue(LPC_LBM_RUN, this, &meta_service::on_load_balance_timer, 0, 1000, 
        _opts.fd_grace_seconds * 2000
        );
    
    end_point primary;
    if (_state->get_meta_server_primary(primary) && primary == primary_address())
    {
        _failure_detector->set_primary(true);
    }   
    else
        _failure_detector->set_primary(false);

    register_rpc_handler(RPC_CM_CALL, "RPC_CM_CALL", &meta_service::on_request);

    _failure_detector->start(
        _opts.fd_check_interval_seconds,
        _opts.fd_beacon_interval_seconds,
        _opts.fd_lease_seconds,
        _opts.fd_grace_seconds,
        false
        );
}

bool meta_service::stop()
{
    if (!_started) return false;
    _started = false;
    _failure_detector->stop();
    delete _failure_detector;
    _failure_detector = nullptr;

    _balancer_timer->cancel(true);
    unregister_rpc_handler(RPC_CM_CALL);
    delete _balancer;
    _balancer = nullptr;
    return true;
}

void meta_service::on_request(message_ptr& msg)
{
    meta_request_header hdr;
    unmarshall(msg, hdr);

    meta_response_header rhdr;
    bool is_primary = _state->get_meta_server_primary(rhdr.primary_address);
    if (is_primary) is_primary = (primary_address() == rhdr.primary_address);
    rhdr.err = ERR_SUCCESS;

    message_ptr resp = msg->create_response();
    if (!is_primary)
    {
        rhdr.err = ERR_TALK_TO_OTHERS;
        
        marshall(resp, rhdr);
    }
    else if (hdr.rpc_tag == RPC_CM_QUERY_NODE_PARTITIONS)
    {
        configuration_query_by_node_request request;
        configuration_query_by_node_response response;
        unmarshall(msg, request);

        query_configuration_by_node(request, response);

        marshall(resp, rhdr);
        marshall(resp, response);
    }

    else if (hdr.rpc_tag == RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX)
    {
        configuration_query_by_index_request request;
        configuration_query_by_index_response response;
        unmarshall(msg, request);

        query_configuration_by_index(request, response);
        
        marshall(resp, rhdr);
        marshall(resp, response);
    }

    else  if (hdr.rpc_tag == RPC_CM_UPDATE_PARTITION_CONFIGURATION)
    {
        update_configuration(msg, resp);
        return;
    }
    
    else
    {
        dassert(false, "unknown rpc tag %x (%s)", hdr.rpc_tag, task_code(hdr.rpc_tag).to_string());
    }

    rpc::reply(resp);
}

// partition server & client => meta server
void meta_service::query_configuration_by_node(configuration_query_by_node_request& request, __out_param configuration_query_by_node_response& response)
{
    _state->query_configuration_by_node(request, response);
}

void meta_service::query_configuration_by_index(configuration_query_by_index_request& request, __out_param configuration_query_by_index_response& response)
{
    _state->query_configuration_by_index(request, response);
}

void meta_service::replay_log(const char* log)
{
    FILE* fp = ::fopen(log, "rb");
    dassert (fp != nullptr, "open operation log %s failed, err = %d", log, errno);

    char buffer[4096]; // enough for holding configuration_update_request
    while (true)
    {
        int32_t len;
        if (1 != ::fread((void*)&len, sizeof(int32_t), 1, fp))
            break;

        dassert(len <= 4096, "");
        auto r = ::fread((void*)buffer, len, 1, fp);
        dassert(r == 1, "log is corrupted");

        blob bb(buffer, 0, len);
        binary_reader reader(bb);

        configuration_update_request request;
        configuration_update_response response;
        unmarshall(reader, request);

        node_states state;
        state.push_back(std::make_pair(request.node, true));
        _state->set_node_state(state);
        _state->update_configuration(request, response);
    }

    ::fclose(fp);
}

void meta_service::update_configuration(message_ptr req, message_ptr resp)
{
    if (_state->freezed())
    {
        meta_response_header rhdr;
        rhdr.err = 0;
        rhdr.primary_address = primary_address();

        configuration_update_request request;
        configuration_update_response response;
        
        unmarshall(req, request);

        response.err = ERR_STATE_FREEZED;
        _state->query_configuration_by_gpid(request.config.gpid, response.config);

        marshall(resp, rhdr);
        marshall(resp, response);

        rpc::reply(resp);
        return;
    }

    auto bb = req->reader().get_remaining_buffer();
    uint64_t offset;
    int len = bb.length() + sizeof(int32_t);
    
    char* buffer = (char*)malloc(len);
    *(int32_t*)buffer = bb.length();
    memcpy(buffer + sizeof(int32_t), bb.data(), bb.length());

    {

        zauto_lock l(_log_lock);
        offset = _offset;
        _offset += len;

        file::write(_log, buffer, len, offset, LPC_CM_LOG_UPDATE, this,
            std::bind(&meta_service::on_log_completed, this, std::placeholders::_1, std::placeholders::_2, buffer, req, resp));
    }
}

void meta_service::on_log_completed(error_code err, int size, char* buffer, message_ptr req, message_ptr resp)
{
    free(buffer);
    dassert(err == ERR_SUCCESS, "log operation failed, cannot proceed, err = %s", err.to_string());

    configuration_update_request request;
    configuration_update_response response;
    unmarshall(req, request);

    update_configuration(request, response);

    meta_response_header rhdr;
    rhdr.err = err;
    rhdr.primary_address = primary_address();

    marshall(resp, rhdr);
    marshall(resp, response);  

    rpc::reply(resp);
}

void meta_service::update_configuration(configuration_update_request& request, __out_param configuration_update_response& response)
{
    _state->update_configuration(request, response);

    if (_started)
    {
        tasking::enqueue(LPC_LBM_RUN, this, std::bind(&meta_service::on_config_changed, this, request.config.gpid));
    }   
}

// local timers
void meta_service::on_load_balance_timer()
{
    // first time is to activate the LB (an initial period of time is presevered for most machine to join in)
    if (!_started)
    {
        _started = true;
        _state->freeze(false);
        return;
    }

    end_point primary;
    if (_state->get_meta_server_primary(primary) && primary == primary_address())
    {
        _failure_detector->set_primary(true);
        _balancer->run();
    }
    else
    {
        _failure_detector->set_primary(false);
    }
}

void meta_service::on_config_changed(global_partition_id gpid)
{
    end_point primary;
    if (_state->get_meta_server_primary(primary) && primary == primary_address())
    {
        _failure_detector->set_primary(true);
        _balancer->run(gpid);
    }
    else
    {
        _failure_detector->set_primary(false);
    }
}
