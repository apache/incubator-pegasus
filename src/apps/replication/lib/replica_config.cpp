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
#include "replica.h"
#include "replication_app_base.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "replication_failure_detector.h"
#include "rpc_replicated.h"

#define __TITLE__ "Configuration"

namespace dsn { namespace replication {

void replica::on_config_proposal(configuration_update_request& proposal)
{
    ddebug(
        "%s: on_config_proposal %s for %s:%u", 
        name(),
        enum_to_string(proposal.type),
        proposal.node.name.c_str(), (int)proposal.node.port
        );

    if (proposal.config.ballot < get_ballot())
        return;

    if (proposal.config.ballot > get_ballot())
    {
        update_configuration(proposal.config);
    }

    switch (proposal.type)
    {
    case CT_ASSIGN_PRIMARY:
        assign_primary(proposal);
        break;
    case CT_ADD_SECONDARY:
        add_potential_secondary(proposal);
        break;
    case CT_DOWNGRADE_TO_SECONDARY:
        downgrade_to_secondary_on_primary(proposal);
        break;
    case CT_DOWNGRADE_TO_INACTIVE:
        downgrade_to_inactive_on_primary(proposal);
        break;
    case CT_REMOVE:
        remove(proposal);
        break;
    default:
        dassert (false, "");
    }
}

void replica::assign_primary(configuration_update_request& proposal)
{
    dassert (proposal.node == address(), "");

    
    if (status() == PS_PRIMARY)
    {
        dwarn(
            "%s: invalid assgin primary proposal as the node is in %s",
            name(),
            enum_to_string(status()));
        return;
    }

    proposal.config.primary = address();
    ReplicaHelper::RemoveNode(address(), proposal.config.secondaries);
    ReplicaHelper::RemoveNode(address(), proposal.config.dropOuts);

    update_configuration_on_meta_server(CT_ASSIGN_PRIMARY, proposal.node, proposal.config);
}

void replica::add_potential_secondary(configuration_update_request& proposal)
{
    if (proposal.config.ballot != get_ballot() || status() != PS_PRIMARY)
        return;

    dassert (proposal.config.gpid == _primary_states.membership.gpid, "");
    dassert (proposal.config.app_type == _primary_states.membership.app_type, "");
    dassert (proposal.config.primary == _primary_states.membership.primary, "");
    dassert (proposal.config.secondaries == _primary_states.membership.secondaries, "");

    // zy: work around for cdt bug
    if (_primary_states.CheckExist(proposal.node, PS_PRIMARY)
        || _primary_states.CheckExist(proposal.node, PS_SECONDARY))
        return;

    dassert (!_primary_states.CheckExist(proposal.node, PS_PRIMARY), "");
    dassert (!_primary_states.CheckExist(proposal.node, PS_SECONDARY), "");

    if (_primary_states.Learners.find(proposal.node) != _primary_states.Learners.end())
    {
        return;
    }

    remote_learner_state state;
    state.prepareStartDecree = invalid_decree;
    state.signature = random64(0, (uint64_t)(-1LL));
    state.timeout_tsk = nullptr; // TODO: add timer for learner task

    _primary_states.Learners[proposal.node] = state;
    _primary_states.Statuses[proposal.node] = PS_POTENTIAL_SECONDARY;

    group_check_request request;
    request.app_type = _primary_states.membership.app_type;
    request.node = proposal.node;
    _primary_states.GetReplicaConfig(proposal.node, request.config);
    request.lastCommittedDecree = last_committed_decree();
    request.learnerSignature = state.signature;

    rpc_typed(proposal.node, RPC_LEARN_ADD_LEARNER, request, gpid_to_hash(get_gpid()));
}

void replica::upgrade_to_secondary_on_primary(const end_point& node)
{
    ddebug(
            "%s: upgrade potential secondary %s:%u to secondary",
            name(),
            node.name.c_str(), (int)node.port
            );

    partition_configuration newConfig = _primary_states.membership;

    // remove from drop out if there
    ReplicaHelper::RemoveNode(node, newConfig.dropOuts);
    // add secondary
    newConfig.secondaries.push_back(node);

    update_configuration_on_meta_server(CT_UPGRADE_TO_SECONDARY, node, newConfig);
}

void replica::downgrade_to_secondary_on_primary(configuration_update_request& proposal)
{
    if (proposal.config.ballot != get_ballot() || status() != PS_PRIMARY)
        return;

    dassert (proposal.config.gpid == _primary_states.membership.gpid, "");
    dassert (proposal.config.app_type == _primary_states.membership.app_type, "");
    dassert (proposal.config.primary == _primary_states.membership.primary, "");
    dassert (proposal.config.secondaries == _primary_states.membership.secondaries, "");
    dassert (proposal.node == proposal.config.primary, "");

    proposal.config.primary = dsn::end_point::INVALID;
    proposal.config.secondaries.push_back(proposal.node);

    update_configuration_on_meta_server(CT_DOWNGRADE_TO_SECONDARY, proposal.node, proposal.config);
}


void replica::downgrade_to_inactive_on_primary(configuration_update_request& proposal)
{
    if (proposal.config.ballot != get_ballot() || status() != PS_PRIMARY)
        return;

    dassert (proposal.config.gpid == _primary_states.membership.gpid, "");
    dassert (proposal.config.app_type == _primary_states.membership.app_type, "");
    dassert (proposal.config.primary == _primary_states.membership.primary, "");
    dassert (proposal.config.secondaries == _primary_states.membership.secondaries, "");

    if (proposal.node == proposal.config.primary)
    {
        proposal.config.primary = dsn::end_point::INVALID;
    }
    else
    {
        auto rt = ReplicaHelper::RemoveNode(proposal.node, proposal.config.secondaries);
        dassert (rt, "");
    }

    proposal.config.dropOuts.push_back(proposal.node);
    update_configuration_on_meta_server(CT_DOWNGRADE_TO_INACTIVE, proposal.node, proposal.config);
}

void replica::remove(configuration_update_request& proposal)
{
    if (proposal.config.ballot != get_ballot() || status() != PS_PRIMARY)
        return;

    dassert (proposal.config.gpid == _primary_states.membership.gpid, "");
    dassert (proposal.config.app_type == _primary_states.membership.app_type, "");
    dassert (proposal.config.primary == _primary_states.membership.primary, "");
    dassert (proposal.config.secondaries == _primary_states.membership.secondaries, "");

    auto st = _primary_states.GetNodeStatus(proposal.node);

    switch (st)
    {
    case PS_PRIMARY:
        dassert (proposal.config.primary == proposal.node, "");
        proposal.config.primary = dsn::end_point::INVALID;
        break;
    case PS_SECONDARY:
        {
        auto rt = ReplicaHelper::RemoveNode(proposal.node, proposal.config.secondaries);
        dassert (rt, "");
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        {
        auto rt = ReplicaHelper::RemoveNode(proposal.node, proposal.config.dropOuts);
        dassert (rt, "");
        }
        break;
    }

    update_configuration_on_meta_server(CT_REMOVE, proposal.node, proposal.config);
}

// from primary
void replica::on_remove(const replica_configuration& request)
{ 
    if (request.ballot < get_ballot())
        return;

    dassert (request.status == PS_INACTIVE, "");
    update_local_configuration(request);
}

void replica::update_configuration_on_meta_server(config_type type, const end_point& node, partition_configuration& newConfig)
{
    newConfig.lastCommittedDecree = last_committed_decree();

    if (type != CT_ASSIGN_PRIMARY)
    {
        dassert (status() == PS_PRIMARY, "");
        dassert (newConfig.ballot == _primary_states.membership.ballot, "");
    }

    // disable 2pc during reconfiguration
    // it is possible to do this only for CT_DOWNGRADE_TO_SECONDARY,
    // we therefore choose to disable 2pc during all reconfiguration types
    // to achieve consistency at the cost of certain write throughput
    update_local_configuration_with_no_ballot_change(PS_INACTIVE);

    message_ptr msg = message::create_request(RPC_CM_CALL, _options.CoordinatorRpcCallTimeoutMs);
    CdtMsgHeader hdr;
    hdr.RpcTag = RPC_CM_UPDATE_PARTITION_CONFIGURATION;
    marshall(msg, hdr);

    std::shared_ptr<configuration_update_request> request(new configuration_update_request);
    request->config = newConfig;
    request->config.ballot++;    
    request->type = type;
    request->node = node;
    marshall(msg, *request);

    if (nullptr != _primary_states.ReconfigurationTask)
    {
        _primary_states.ReconfigurationTask->cancel(true);
    }

    //if (dsn::service::system::Mode() == SM_Simulation)
    //{
    //    // always success for the time being
    //    ConfigurationUpdateResponse resp;
    //    resp.err = ERR_SUCCESS;
    //    resp.config = request->config;

    //    message_ptr msg2 = msg->create_response();
    //    marshall(msg2, resp);
    //    
    //    auto bb2 = msg2->get_output_buffer();
    //    message_ptr response(new message(bb2));

    //    _primary_states.ReconfigurationTask = enqueue_task(
    //        LPC_SIM_UPDATE_PARTITION_CONFIGURATION_REPLY,
    //        std::bind(&replica::on_update_configuration_on_meta_server_reply, this, ERR_SUCCESS, msg, response, request),
    //        gpid_to_hash(get_gpid()),
    //        5
    //        );
    //}
    //else
    {
        _primary_states.ReconfigurationTask = rpc_replicated(
            _stub->_livenessMonitor->current_server_contact(),
            _stub->_livenessMonitor->get_servers(),
            msg,
            this,
            std::bind(&replica::on_update_configuration_on_meta_server_reply, this, 
                std::placeholders::_1, 
                std::placeholders::_2, 
                std::placeholders::_3, 
                request),
            gpid_to_hash(get_gpid())
            );
    }
}


void replica::on_update_configuration_on_meta_server_reply(error_code err, message_ptr& request, message_ptr& response, std::shared_ptr<configuration_update_request> req)
{
    if (PS_INACTIVE != status() || _stub->is_connected() == false)
    {
        return;
    }

    if (err)
    {
        _primary_states.ReconfigurationTask = rpc_replicated(
            _stub->_livenessMonitor->current_server_contact(),
            _stub->_livenessMonitor->get_servers(),
            request,
            this,
            std::bind(&replica::on_update_configuration_on_meta_server_reply, this, 
                std::placeholders::_1, 
                std::placeholders::_2, 
                std::placeholders::_3, 
                req),
            gpid_to_hash(get_gpid())
            );
        return;
    }

    ConfigurationUpdateResponse resp;
    unmarshall(response, resp);    

    ddebug(
        "%s: update configuration reply with err %x, ballot %lld, local %lld",
        name(),
        resp.err,
        resp.config.ballot,
        get_ballot()
        );
    
    if (resp.config.ballot < get_ballot())
        return;
    
    // post-update work items?
    if (resp.err == ERR_SUCCESS)
    {        
        dassert (req->config.gpid == resp.config.gpid, "");
        dassert (req->config.app_type == resp.config.app_type, "");
        dassert (req->config.primary == resp.config.primary, "");
        dassert (req->config.secondaries == resp.config.secondaries, "");

        switch (req->type)
        {
        case CT_ASSIGN_PRIMARY:
        case CT_DOWNGRADE_TO_SECONDARY:
        case CT_DOWNGRADE_TO_INACTIVE:
        case CT_UPGRADE_TO_SECONDARY:
            break;
        case CT_REMOVE:
            if (req->node != address())
            {
                replica_configuration rconfig;
                ReplicaHelper::GetReplicaConfig(resp.config, req->node, rconfig);
                rpc_typed(req->node, RPC_REMOVE_REPLICA, rconfig, gpid_to_hash(get_gpid()));
            }
            break;
        default:
            dassert (false, "");
        }
    }
    
    update_configuration(resp.config);
}

void replica::update_configuration(const partition_configuration& config)
{
    dassert (config.ballot >= get_ballot(), "");
    
    replica_configuration rconfig;
    ReplicaHelper::GetReplicaConfig(config, address(), rconfig);

    if (config.ballot > get_ballot() || status() != rconfig.status)
    {
        _primary_states.ResetMembership(config, config.primary != address());
    }

    update_local_configuration(rconfig);
}

void replica::update_local_configuration(const replica_configuration& config)
{
    dassert (config.ballot >= get_ballot(), "");
    dassert (config.gpid == get_gpid(), "");

    partition_status oldStatus = status();
    ballot oldBallot = get_ballot();

    if (oldStatus == config.status && oldBallot == config.ballot)
        return;

    if (oldStatus == PS_ERROR && (config.status == PS_SECONDARY || config.status == PS_PRIMARY || config.status == PS_INACTIVE))
    {
        ddebug(
            "%s: status change from %s @ %lld to %s @ %lld is not allowed",
            name(),
            enum_to_string(oldStatus),
            oldBallot,
            enum_to_string(config.status),
            config.ballot
            );
        return;
    }

    if (oldStatus == PS_POTENTIAL_SECONDARY && (config.status == PS_ERROR || config.status == PS_INACTIVE))
    {
        if (!_potential_secondary_states.Cleanup(false))
        {
            dwarn(
                "%s: status change from %s @ %lld to %s @ %lld is not allowed coz learning remote state is still running",
                name(),
                enum_to_string(oldStatus),
                oldBallot,
                enum_to_string(config.status),
                config.ballot
                );
            return;
        }
    }

    uint64_t oldTs = _last_config_change_time_ms;
    _config = config;
    _last_config_change_time_ms =now_ms();
    dassert (max_prepared_decree() >= last_committed_decree(), "");
    
    switch (oldStatus)
    {
    case PS_PRIMARY:
        cleanup_preparing_mutations(true);
        switch (config.status)
        {
        case PS_PRIMARY:
            replay_prepare_list();
            break;
        case PS_INACTIVE:
            _primary_states.Cleanup(oldBallot != config.ballot);
            break;
        case PS_SECONDARY:
        case PS_ERROR:
            _primary_states.Cleanup();
            break;
        case PS_POTENTIAL_SECONDARY:
            dassert (false, "invalid execution path");
            break;
        default:
            dassert (false, "invalid execution path");
        }        
        break;
    case PS_SECONDARY:
        switch (config.status)
        {
        case PS_PRIMARY:
            init_group_check();
            replay_prepare_list();
            break;
        case PS_SECONDARY:
            break;
        case PS_POTENTIAL_SECONDARY:
            // InActive in config
            break;
        case PS_INACTIVE:
            break;
        case PS_ERROR:
            break;
        default:
            dassert (false, "invalid execution path");
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        switch (config.status)
        {
        case PS_PRIMARY:
            dassert (false, "invalid execution path");
            break;
        case PS_SECONDARY:
            _prepare_list->truncate(_app->last_committed_decree());            
            _potential_secondary_states.Cleanup(true);
            break;
        case PS_POTENTIAL_SECONDARY:
            break;
        case PS_INACTIVE:
        case PS_ERROR:
            _prepare_list->reset(_app->last_committed_decree());
            _potential_secondary_states.Cleanup(true);
            break;
        default:
            dassert (false, "invalid execution path");
        }
        break;
    case PS_INACTIVE:
        switch (config.status)
        {
        case PS_PRIMARY:
            init_group_check();
            replay_prepare_list();
            break;
        case PS_SECONDARY:            
            break;
        case PS_POTENTIAL_SECONDARY:
            break;
        case PS_INACTIVE:
            break;
        case PS_ERROR:
            break;
        default:
            dassert (false, "invalid execution path");
        }
        break;
    case PS_ERROR:
        switch (config.status)
        {
        case PS_PRIMARY:
            dassert (false, "invalid execution path");
            break;
        case PS_SECONDARY:
            dassert (false, "invalid execution path");
            break;
        case PS_POTENTIAL_SECONDARY:
            break;
        case PS_INACTIVE:
            dassert (false, "invalid execution path");
            break;
        case PS_ERROR:
            break;
        default:
            dassert (false, "invalid execution path");
        }
        break;
    default:
        dassert (false, "invalid execution path");
    }

    if (status() != oldStatus)
    {
        ddebug(
            "%s: status change %s @ %lld => %s @ %lld, pre(%llu, %llu), app(%llu, %llu), duration=%llu ms",
            name(),
            enum_to_string(oldStatus),
            oldBallot,
            enum_to_string(status()),
            get_ballot(),
            _prepare_list->max_decree(),
            _prepare_list->last_committed_decree(),
            _app->last_committed_decree(),
            _app->last_durable_decree(),
            _last_config_change_time_ms - oldTs
            );

        bool isClosing = (status() == PS_ERROR || (status() == PS_INACTIVE && get_ballot() > oldBallot));
        _stub->notify_replica_state_update(config, isClosing);

        if (isClosing)
        {
            _stub->begin_close_replica(this);
        }
    }
    else
    {
        _stub->notify_replica_state_update(config, false);
    }
}

void replica::update_local_configuration_with_no_ballot_change(partition_status s)
{
    if (status() == s)
        return;

    auto config = _config;
    config.status = s;
    update_local_configuration(config);
}

void replica::on_config_sync(const partition_configuration& config)
{
    ddebug( "%s: configuration sync", name());

    if (config.ballot >= get_ballot())
    {
        update_configuration(config);
    }
}

void replica::replay_prepare_list()
{
    decree start = last_committed_decree() + 1;
    decree end = _prepare_list->max_decree();

    ddebug(
            "%s: replay prepare list from %lld to %lld, ballot = %lld",
            name(),
            start,
            end,
            get_ballot()
            );

    for (decree decree = start; decree <= end; decree++)
    {
        mutation_ptr old = _prepare_list->get_mutation_by_decree(decree);
        mutation_ptr mu = new_mutation(decree);

        if (old != nullptr)
        {
            mu->data.updates = old->data.updates;
            mu->client_requests = old->client_requests;

            dbg_dassert (mu->client_requests.size() == old->client_requests.size());
            dbg_dassert (mu->data.updates.size() == old->data.updates.size());
        }
        else
        {
            ddebug(
                "%s: emit empty mutation %s when replay prepare list",
                name(),
                mu->name()
                );
        }

        init_prepare(mu);
    }
}

}} // namespace
