/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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

meta_service::meta_service(server_state* state, configuration_ptr c)
: _state(state), serverlet("meta_service")
{
    _balancer = nullptr;
    _failure_detector = nullptr;

    _opts.initialize(c);
}

meta_service::~meta_service(void)
{
}

void meta_service::start()
{
    _balancer = new load_balancer(_state);            
    _failure_detector = new meta_server_failure_detector(_state);
    _balancer_timer = tasking::enqueue(LPC_LBM_RUN, this, &meta_service::on_load_balance_timer, 0, 1000, 5000);
    register_rpc_handler(RPC_CM_CALL, "RPC_CM_CALL", &meta_service::on_request);

    end_point primary;
    if (_state->get_meta_server_primary(primary) && primary == primary_address())
        _failure_detector->set_primary(true);
    else
        _failure_detector->set_primary(false);

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
    bool isPrimary = _state->get_meta_server_primary(rhdr.primary_address);
    if (isPrimary) isPrimary = (primary_address() == rhdr.primary_address);
    rhdr.err = ERR_SUCCESS;
    
    message_ptr resp = msg->create_response();

    if (!isPrimary)
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

    else if (hdr.rpc_tag == RPC_CM_UPDATE_PARTITION_CONFIGURATION)
    {
        configuration_update_request request;
        configuration_update_response response;
        unmarshall(msg, request);

        update_configuration(request, response);
        
        marshall(resp, rhdr);
        marshall(resp, response);
    }

    else
    {
        dassert (false, "unknown rpc tag %x", hdr.rpc_tag);
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

void meta_service::update_configuration(configuration_update_request& request, __out_param configuration_update_response& response)
{
    _state->update_configuration(request, response);

    tasking::enqueue(LPC_LBM_RUN, this, std::bind(&meta_service::on_config_changed, this, request.config.gpid));
}

// local timers
void meta_service::on_load_balance_timer()
{
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
