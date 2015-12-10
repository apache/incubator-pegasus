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

#include "meta_server_failure_detector.h"
#include "server_state.h"
#include "meta_service.h"
#include <dsn/internal/factory_store.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "meta.server.FD"

meta_server_failure_detector::meta_server_failure_detector(server_state* state, meta_service* svc)
{
    _state = state;
    _svc = svc;
    _is_primary = false;

    const char* distributed_lock_service_name = dsn_config_get_value_string(
        "meta_server",
        "distributed_lock_service_name",
        "distributed_lock_service_simple",
        "the distributed_lock_service provider name"
        );

    _lock_svc = dsn::utils::factory_store< ::dsn::dist::distributed_lock_service>::create(
        distributed_lock_service_name,
        PROVIDER_TYPE_MAIN
        );
    _lock_svc->initialize();
    _primary_lock_id = "dsn.meta.server.leader";
    _local_owner_id = primary_address().to_string();
}

meta_server_failure_detector::~meta_server_failure_detector(void)
{
    auto t = _lock_grant_task;
    if (t) t->cancel(true);
    t = _lock_expire_task;
    if (t) t->cancel(true);

    delete _lock_svc;
}

void meta_server_failure_detector::on_worker_disconnected(const std::vector< ::dsn::rpc_address>& nodes)
{
    if (!is_primary())
    {
        return;
    }

    if (!_svc->_started)
    {
        return;
    }

    node_states states;
    for (auto& n : nodes)
    {
        states.push_back(std::make_pair(n, false));

        dwarn("client expired: %s", n.to_string());
    }
    
    machine_fail_updates pris;
    _state->set_node_state(states, &pris);
    
    for (auto& pri : pris)
    {
        dinfo("%d.%d primary node for %s is gone, update configuration on meta server", 
            pri.first.app_id,
            pri.first.pidx,
            pri.second->node.to_string()
            );
        _svc->update_configuration_on_machine_failure(pri.second);
    }
}

void meta_server_failure_detector::on_worker_connected(::dsn::rpc_address node)
{
    if (!is_primary())
    {
        return;
    }

    node_states states;
    states.push_back(std::make_pair(node, true));

    dwarn("Client reconnected",
        "Client %s", node.to_string());

    _state->set_node_state(states, nullptr);
}

DEFINE_TASK_CODE(LPC_META_SERVER_LEADER_LOCK_CALLBACK, TASK_PRIORITY_COMMON, THREAD_POOL_FD)
DEFINE_TASK_CODE_RPC(RPC_CM_QUERY_LEADER_LOCK, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_CM_QUERY_LEADER_LOCK_TIMER, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

void meta_server_failure_detector::acquire_leader_lock()
{
    //
    // lets' first set up a timer to query the leader
    // so as to tell other nodes who is the leader in case they query
    //
    int leader_lock_query_interval_seconds = (int)dsn_config_get_value_uint64(
        "meta_server",
        "leader_lock_query_interval_seconds",
        5, // 5 seconds
        "period (seconds) for meta server to query the leader before itself becomes the leader"
        );

    task_ptr query_leader_task;
    task_ptr query_leader_timer = tasking::enqueue(
        LPC_CM_QUERY_LEADER_LOCK_TIMER,
        this,
        [this, &query_leader_task]()
        {
            if (query_leader_task != nullptr)
                return;

            query_leader_task = _lock_svc->query_lock(
                _primary_lock_id,
                RPC_CM_QUERY_LEADER_LOCK,
                [this, &query_leader_task](error_code ec, const std::string& owner_id, uint64_t version)
                {
                    if (ec == ERR_OK)
                    {
                        rpc_address addr;
                        if (addr.from_string_ipv4(owner_id.c_str()))
                        {
                            dassert(primary_address() == addr, "");
                            set_primary(addr);
                        }
                    }
                    query_leader_task = nullptr;
                }
            );
        },
        0,
        0,
        leader_lock_query_interval_seconds * 1000
        );
        
    //
    // try to get the leader lock until it is done
    //
    while (true)
    {
        error_code err;
        auto tasks =
            _lock_svc->lock(
            _primary_lock_id,
            _local_owner_id,
            true,

            // lock granted
            LPC_META_SERVER_LEADER_LOCK_CALLBACK,
            [this, &err](error_code ec, const std::string& owner, uint64_t version)
        {
            err = ec;
        },

            // lease expire
            LPC_META_SERVER_LEADER_LOCK_CALLBACK,
            [this](error_code ec, const std::string& owner, uint64_t version)
        {
            // let's take the easy way right now
            dsn_terminate();

            // reset primary
            //rpc_address addr;
            //set_primary(addr);
            //_state->on_become_non_leader();

            //// set another round of service
            //acquire_leader_lock();            
        }
        );

        _lock_grant_task = tasks.first;
        _lock_expire_task = tasks.second;

        _lock_grant_task->wait();
        if (err == ERR_OK)
        {
            rpc_address addr;
            if (addr.from_string_ipv4(_local_owner_id.c_str()))
            {
                query_leader_timer->cancel(true);
                query_leader_timer = nullptr;

                task_ptr t = query_leader_task;
                if (t != nullptr)
                {
                    t->cancel(true);
                    query_leader_task = nullptr;
                }

                dassert(primary_address() == addr, "");
                set_primary(addr);
                break;
            }
        }
    }
}

void meta_server_failure_detector::set_primary(rpc_address primary)
{
    bool old = _is_primary;
    {
        utils::auto_lock<zlock> l(_primary_address_lock);
        _primary_address = primary;
        _is_primary = (primary == primary_address());
    }

    if (!old && _is_primary)
    {
        node_states ns;
        _state->get_node_state(ns);

        for (auto& pr : ns)
        {
            register_worker(pr.first, pr.second);
        }
    }

    if (old && !_is_primary)
    {
        clear_workers();
    }
}


void meta_server_failure_detector::on_ping(const fd::beacon_msg& beacon, ::dsn::rpc_replier<fd::beacon_ack>& reply)
{
    fd::beacon_ack ack;
    ack.this_node = beacon.to;
    if (!is_primary())
    {
        ack.time = beacon.time;
        ack.is_master = false;
        ack.primary_node = _primary_address;
    }
    else
    {
        failure_detector::on_ping_internal(beacon, ack);
        ack.primary_node = primary_address();
    }

    reply(ack);
}

