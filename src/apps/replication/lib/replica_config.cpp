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

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "replication_failure_detector.h"
#include "rpc_replicated.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "Configuration"

namespace dsn { namespace replication {

void replica::on_config_proposal(configuration_update_request& proposal)
{
    check_hashed_access();

    ddebug(
        "%s: on_config_proposal %s for %s", 
        name(),
        enum_to_string(proposal.type),
        proposal.node.to_string()
        );

    if (proposal.config.ballot < get_ballot())
    {
        dwarn(
            "%s: on_config_proposal is out-dated, %lld vs %lld",
            name(),
            proposal.config.ballot,
            get_ballot()
            );
        return;
    }   

    if (_primary_states.reconfiguration_task != nullptr)
    {
        dinfo(
            "%s: reconfiguration on the way, skip the incoming proposal",
            name()
            );
        return;
    }

    if (proposal.config.ballot > get_ballot())
    {
        if (!update_configuration(proposal.config))
        {
            // is closing or update failed
            return;
        }
    }
    
    switch (proposal.type)
    {
    case CT_ASSIGN_PRIMARY:
    case CT_UPGRADE_TO_PRIMARY:
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
    dassert(proposal.node == primary_address(), "");

    if (status() == PS_PRIMARY)
    {
        dwarn(
            "%s: invalid assgin primary proposal as the node is in %s",
            name(),
            enum_to_string(status()));
        return;
    }

    if (proposal.type == CT_UPGRADE_TO_PRIMARY 
        && (status() != PS_SECONDARY || _secondary_states.checkpoint_task != nullptr))
    {
        dwarn(
            "%s: invalid upgrade to primary proposal as the node is in %s or during checkpointing",
            name(),
            enum_to_string(status()));

        // TODO: tell meta server so new primary is built more quickly
        return;
    }

    proposal.config.primary = primary_address();
    replica_helper::remove_node(primary_address(), proposal.config.secondaries);

    update_configuration_on_meta_server(proposal.type, proposal.node, proposal.config);
}

void replica::add_potential_secondary(configuration_update_request& proposal)
{
    if (status() != PS_PRIMARY)
    {
        return;
    }   

    dassert (proposal.config.ballot == get_ballot(), "");
    dassert (proposal.config.gpid == _primary_states.membership.gpid, "");
    dassert (proposal.config.app_type == _primary_states.membership.app_type, "");
    dassert (proposal.config.primary == _primary_states.membership.primary, "");
    dassert (proposal.config.secondaries == _primary_states.membership.secondaries, "");
    dassert (!_primary_states.check_exist(proposal.node, PS_PRIMARY), "");
    dassert (!_primary_states.check_exist(proposal.node, PS_SECONDARY), "");

    remote_learner_state state;
    state.prepare_start_decree = invalid_decree;
    state.timeout_task = nullptr; // TODO: add timer for learner task

    auto it = _primary_states.learners.find(proposal.node);
    if (it != _primary_states.learners.end())
    {
        state.signature = it->second.signature;
    }
    else
    {
        state.signature = random64(0, (uint64_t)(-1LL));
        _primary_states.learners[proposal.node] = state;
        _primary_states.statuses[proposal.node] = PS_POTENTIAL_SECONDARY;
    }

    group_check_request request;
    request.app_type = _primary_states.membership.app_type;
    request.node = proposal.node;
    _primary_states.get_replica_config(proposal.node, request.config);
    request.last_committed_decree = last_committed_decree();
    request.learner_signature = state.signature;

    rpc::call_one_way_typed(proposal.node, RPC_LEARN_ADD_LEARNER, request, gpid_to_hash(get_gpid()));
}

void replica::upgrade_to_secondary_on_primary(::dsn::rpc_address node)
{
    ddebug(
            "%s: upgrade potential secondary %s to secondary",
            name(),
            node.to_string()
            );

    partition_configuration newConfig = _primary_states.membership;

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

    proposal.config.primary.set_invalid();
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
        proposal.config.primary.set_invalid();
    }
    else
    {
        auto rt = replica_helper::remove_node(proposal.node, proposal.config.secondaries);
        dassert (rt, "");
    }

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

    auto st = _primary_states.get_node_status(proposal.node);

    switch (st)
    {
    case PS_PRIMARY:
        dassert (proposal.config.primary == proposal.node, "");
        proposal.config.primary.set_invalid();
        break;
    case PS_SECONDARY:
        {
        auto rt = replica_helper::remove_node(proposal.node, proposal.config.secondaries);
        dassert (rt, "");
        }
        break;
    case PS_POTENTIAL_SECONDARY:
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

void replica::update_configuration_on_meta_server(config_type type, ::dsn::rpc_address node, partition_configuration& newConfig)
{
    newConfig.last_committed_decree = last_committed_decree();

    if (type != CT_ASSIGN_PRIMARY && type != CT_UPGRADE_TO_PRIMARY)
    {
        dassert (status() == PS_PRIMARY, "");
        dassert (newConfig.ballot == _primary_states.membership.ballot, "");
    }

    // disable 2pc during reconfiguration
    // it is possible to do this only for CT_DOWNGRADE_TO_SECONDARY,
    // but we choose to disable 2pc during all reconfiguration types
    // for simplicity at the cost of certain write throughput
    update_local_configuration_with_no_ballot_change(PS_INACTIVE);
    set_inactive_state_transient(true);

    dsn_message_t msg = dsn_msg_create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION, 0, 0);
    
    std::shared_ptr<configuration_update_request> request(new configuration_update_request);
    request->config = newConfig;
    request->config.ballot++;
    request->type = type;
    request->node = node;

    ::marshall(msg, *request);

    if (nullptr != _primary_states.reconfiguration_task)
    {
        _primary_states.reconfiguration_task->cancel(true);
    }

    rpc_address target(_stub->_failure_detector->get_servers());
    _primary_states.reconfiguration_task = rpc::call(
        target,
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

void replica::on_update_configuration_on_meta_server_reply(error_code err, dsn_message_t request, dsn_message_t response, std::shared_ptr<configuration_update_request> req)
{
    check_hashed_access();

    if (PS_INACTIVE != status() || _stub->is_connected() == false)
    {
        _primary_states.reconfiguration_task = nullptr;
        err.end_tracking();
        return;
    }

    configuration_update_response resp;
    if (err == ERR_OK)
    {
        ::unmarshall(response, resp);
        err = resp.err;
    }

    if (err != ERR_OK)
    {
        ddebug(
            "%s: update configuration reply with err %s, request ballot %lld",
            name(),
            err.to_string(),
            req->config.ballot
            );

        if (err != ERR_INVALID_VERSION)
        {
            rpc_address target(_stub->_failure_detector->get_servers());
            dsn_msg_add_ref(request); // added for another round of rpc::call
            _primary_states.reconfiguration_task = rpc::call(
                target,
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
    }

    ddebug(
        "%s: update configuration reply with err %s, ballot %lld, local %lld",
        name(),
        resp.err.to_string(),
        resp.config.ballot,
        get_ballot()
        );
    
    if (resp.config.ballot < get_ballot())
    {
        _primary_states.reconfiguration_task = nullptr;
        return;
    }        
    
    // post-update work items?
    if (resp.err == ERR_OK)
    {        
        dassert (req->config.gpid == resp.config.gpid, "");
        dassert (req->config.app_type == resp.config.app_type, "");
        dassert (req->config.primary == resp.config.primary, "");
        dassert (req->config.secondaries == resp.config.secondaries, "");

        switch (req->type)
        {        
        case CT_UPGRADE_TO_PRIMARY:
            _primary_states.last_prepare_decree_on_new_primary = _prepare_list->max_decree();
            break;
        case CT_ASSIGN_PRIMARY:
            _primary_states.last_prepare_decree_on_new_primary = 0;
            break;
        case CT_DOWNGRADE_TO_SECONDARY:
        case CT_DOWNGRADE_TO_INACTIVE:
        case CT_UPGRADE_TO_SECONDARY:
            break;
        case CT_REMOVE:
            if (req->node != primary_address())
            {
                replica_configuration rconfig;
                replica_helper::get_replica_config(resp.config, req->node, rconfig);
                rpc::call_one_way_typed(req->node, RPC_REMOVE_REPLICA, rconfig, gpid_to_hash(get_gpid()));
            }
            break;
        default:
            dassert (false, "");
        }
    }
    
    update_configuration(resp.config);
    _primary_states.reconfiguration_task = nullptr;
}

bool replica::update_configuration(const partition_configuration& config)
{
    dassert (config.ballot >= get_ballot(), "");
    
    replica_configuration rconfig;
    replica_helper::get_replica_config(config, primary_address(), rconfig);

    if (rconfig.status == PS_PRIMARY &&
        (rconfig.ballot > get_ballot() || status() != PS_PRIMARY)
        )
    {
        _primary_states.reset_membership(config, config.primary != primary_address());
    }

    if (config.ballot > get_ballot() ||
        is_same_ballot_status_change_allowed(status(), rconfig.status)
        )
    {
        return update_local_configuration(rconfig, true);
    }
    else
        return false;
}

bool replica::is_same_ballot_status_change_allowed(partition_status olds, partition_status news)
{
    return
        // add learner
        (olds == PS_INACTIVE && news == PS_POTENTIAL_SECONDARY)

        // learner ready for secondary
        || (olds == PS_POTENTIAL_SECONDARY && news == PS_SECONDARY)

        // meta server come back
        || (olds == PS_INACTIVE && news == PS_SECONDARY && _inactive_is_transient)

        // meta server come back
        || (olds == PS_INACTIVE && news == PS_PRIMARY && _inactive_is_transient)

        // no change
        || (olds == news)
        ;
}

bool replica::update_local_configuration(const replica_configuration& config, bool same_ballot/* = false*/)
{
    dassert(config.ballot > get_ballot()
        || (same_ballot && config.ballot == get_ballot()), "");
    dassert (config.gpid == get_gpid(), "");

    partition_status old_status = status();
    ballot old_ballot = get_ballot();

    // skip unncessary configuration change
    if (old_status == config.status && old_ballot == config.ballot)
        return true;

    // skip invalid change
    switch (old_status)
    {
    case PS_ERROR:
        {
            ddebug(
                "%s: status change from %s @ %lld to %s @ %lld is not allowed",
                name(),
                enum_to_string(old_status),
                old_ballot,
                enum_to_string(config.status),
                config.ballot
                );
            return false;
        }
        break;
    case PS_INACTIVE:
        if ((config.status == PS_PRIMARY || config.status == PS_SECONDARY)
            && !_inactive_is_transient)
        {
            ddebug(
                "%s: status change from %s @ %lld to %s @ %lld is not allowed when inactive state is not transient",
                name(),
                enum_to_string(old_status),
                old_ballot,
                enum_to_string(config.status),
                config.ballot
                );
            return false;
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        if (config.status == PS_ERROR || config.status == PS_INACTIVE)
        {
            if (!_potential_secondary_states.cleanup(false))
            {
                dwarn(
                    "%s: status change from %s @ %lld to %s @ %lld is not allowed coz learning remote state is still running",
                    name(),
                    enum_to_string(old_status),
                    old_ballot,
                    enum_to_string(config.status),
                    config.ballot
                    );
                return false;
            }
        }
        break;
    case PS_SECONDARY:
        if (config.status != PS_SECONDARY
            && _secondary_states.checkpoint_task != nullptr)
        {
            dwarn(
                "%s: status change from %s @ %lld to %s @ %lld is not allowed coz checkpointing %p is still running",
                name(),
                enum_to_string(old_status),
                old_ballot,
                enum_to_string(config.status),
                config.ballot,
                _secondary_states.checkpoint_task->native_handle()
                );
            return false;
        }
        break;
    }

    uint64_t oldTs = _last_config_change_time_ms;
    _config = config;
    _last_config_change_time_ms =now_ms();
    dassert (max_prepared_decree() >= last_committed_decree(), "");
    
    switch (old_status)
    {
    case PS_PRIMARY:
        cleanup_preparing_mutations(true);
        switch (config.status)
        {
        case PS_PRIMARY:
            replay_prepare_list();
            break;
        case PS_INACTIVE:
            _primary_states.cleanup(old_ballot != config.ballot);
            break;
        case PS_SECONDARY:
        case PS_ERROR:
            _primary_states.cleanup();
            break;
        case PS_POTENTIAL_SECONDARY:
            dassert (false, "invalid execution path");
            break;
        default:
            dassert (false, "invalid execution path");
        }        
        break;
    case PS_SECONDARY:
        cleanup_preparing_mutations(true);
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
            _potential_secondary_states.cleanup(true);
            check_state_completeness();
            break;
        case PS_POTENTIAL_SECONDARY:
            break;
        case PS_INACTIVE:
            _potential_secondary_states.cleanup(true);
            break;
        case PS_ERROR:
            _prepare_list->reset(_app->last_committed_decree());
            _potential_secondary_states.cleanup(true);
            break;
        default:
            dassert (false, "invalid execution path");
        }
        break;
    case PS_INACTIVE:        
        switch (config.status)
        {
        case PS_PRIMARY:
            _inactive_is_transient = false;
            init_group_check();
            replay_prepare_list();
            break;
        case PS_SECONDARY:            
            _inactive_is_transient = false;
            break;
        case PS_POTENTIAL_SECONDARY:
            _inactive_is_transient = false;
            break;
        case PS_INACTIVE:
            break;
        case PS_ERROR:
            _inactive_is_transient = false;
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
            dassert(false, "invalid execution path");
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

    dwarn(
        "%s: status change %s @ %lld => %s @ %lld, pre(%llu, %llu), app(%llu, %llu), duration=%llu ms",
        name(),
        enum_to_string(old_status),
        old_ballot,
        enum_to_string(status()),
        get_ballot(),
        _prepare_list->max_decree(),
        _prepare_list->last_committed_decree(),
        _app->last_committed_decree(),
        _app->last_durable_decree(),
        _last_config_change_time_ms - oldTs
        );

    if (status() != old_status)
    {
        bool isClosing = (status() == PS_ERROR || (status() == PS_INACTIVE && get_ballot() > old_ballot));
        _stub->notify_replica_state_update(config, isClosing);

        if (isClosing)
        {
            ddebug("%s: being close ...", name());
            _stub->begin_close_replica(this);
            return false;
        }
    }
    else
    {
        _stub->notify_replica_state_update(config, false);
    }
    return true;
}

bool replica::update_local_configuration_with_no_ballot_change(partition_status s)
{
    if (status() == s)
        return false;

    auto config = _config;
    config.status = s;
    return update_local_configuration(config, true);
}

void replica::on_config_sync(const partition_configuration& config)
{
    ddebug( "%s: configuration sync", name());

    // no outdated update
    if (config.ballot < get_ballot())
        return;

    if (status() == PS_PRIMARY || nullptr != _primary_states.reconfiguration_task)
    {
        // nothing to do as pirmary always holds the truth
    }
    else
    {
        update_configuration(config);

        if (status() == PS_INACTIVE && !_inactive_is_transient)
        {
            if (config.primary == primary_address() // dead primary
                || config.primary.is_invalid() // primary is dead (otherwise let primary remove this)
                )
            {
                _stub->remove_replica_on_meta_server(config);
            }
        }
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
            mu->copy_from(old);
        }
        else
        {
            mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);

            ddebug(
                "%s: emit empty mutation %lld when replay prepare list",
                name(),
                decree
                );
        }

        init_prepare(mu);
    }
}

}} // namespace
