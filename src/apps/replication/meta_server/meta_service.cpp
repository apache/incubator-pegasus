/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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
: _state(state), serviceletex("meta_service")
{
    _balancer = nullptr;
    _livenessMonitor = nullptr;

    _opts.initialize(c);
}

meta_service::~meta_service(void)
{
}

void meta_service::start()
{
    _balancer = new load_balancer(_state);
    _livenessMonitor = new meta_server_failure_detector(_state);
    register_rpc_handler(RPC_CM_CALL, "RPC_CM_CALL", &meta_service::OnMetaServiceRequest);
    _balancerTimer = enqueue_task(LPC_LBM_RUN, &meta_service::OnLoadBalancerTimer, 0, 1000, 5000);

    end_point primary;
    if (_state->GetMetaServerPrimary(primary) && primary == address())
        _livenessMonitor->set_primary(true);
    else
        _livenessMonitor->set_primary(false);

    _livenessMonitor->init(_opts.FD_check_interval_seconds,
        _opts.FD_beacon_interval_seconds,
        _opts.FD_lease_seconds,
        _opts.FD_grace_seconds,
        false
        );

    _livenessMonitor->start();
}

bool meta_service::stop()
{
    _livenessMonitor->stop();
    _livenessMonitor->uninit();
    delete _livenessMonitor;
    _livenessMonitor = nullptr;

    _balancerTimer->cancel(true);
    unregister_rpc_handler(RPC_CM_CALL);
    delete _balancer;
    _balancer = nullptr;
    return true;
}

void meta_service::OnMetaServiceRequest(message_ptr& msg)
{
    CdtMsgHeader hdr;
    unmarshall(msg, hdr);

    CdtMsgResponseHeader rhdr;
    bool isPrimary = _state->GetMetaServerPrimary(rhdr.PrimaryAddress);
    if (isPrimary) isPrimary = (address() == rhdr.PrimaryAddress);
    rhdr.Err = ERR_SUCCESS;
    
    message_ptr resp = msg->create_response();

    if (!isPrimary)
    {
        rhdr.Err = ERR_TALK_TO_OTHERS;
        
        marshall(resp, rhdr);
    }
    else if (hdr.RpcTag == RPC_CM_QUERY_NODE_PARTITIONS)
    {
        ConfigurationNodeQueryRequest request;
        ConfigurationNodeQueryResponse response;
        unmarshall(msg, request);

        OnQueryConfig(request, response);

        marshall(resp, rhdr);
        marshall(resp, response);
    }

    else if (hdr.RpcTag == RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX)
    {
        QueryConfigurationByIndexRequest request;
        QueryConfigurationByIndexResponse response;
        unmarshall(msg, request);

        DoQueryConfigurationByIndexRequest(request, response);
        
        marshall(resp, rhdr);
        marshall(resp, response);
    }

    else if (hdr.RpcTag == RPC_CM_UPDATE_PARTITION_CONFIGURATION)
    {
        configuration_update_request request;
        ConfigurationUpdateResponse response;
        unmarshall(msg, request);

        update_configuration(request, response);
        
        marshall(resp, rhdr);
        marshall(resp, response);
    }

    else
    {
        dassert(false, "unknown rpc tag %x", hdr.RpcTag);
    }

    rpc_response(resp);
}

// partition server & client => meta server
void meta_service::OnQueryConfig(ConfigurationNodeQueryRequest& request, __out_param ConfigurationNodeQueryResponse& response)
{
    _state->OnQueryConfig(request, response);
}

void meta_service::DoQueryConfigurationByIndexRequest(QueryConfigurationByIndexRequest& request, __out_param QueryConfigurationByIndexResponse& response)
{
    _state->DoQueryConfigurationByIndexRequest(request, response);
}

void meta_service::update_configuration(configuration_update_request& request, __out_param ConfigurationUpdateResponse& response)
{
    _state->update_configuration(request, response);
}

// local timers
void meta_service::OnLoadBalancerTimer()
{
    end_point primary;
    if (_state->GetMetaServerPrimary(primary) && primary == address())
    {
        _livenessMonitor->set_primary(true);
        _balancer->run();
    }
    else
    {
        _livenessMonitor->set_primary(false);
    }
}
