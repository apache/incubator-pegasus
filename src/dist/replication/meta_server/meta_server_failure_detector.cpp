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

#include <dsn/internal/factory_store.h>
#include "meta_server_failure_detector.h"
#include "server_state.h"
#include "meta_service.h"
#include "meta_options.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "meta.server.FD"

namespace dsn { namespace replication {

meta_server_failure_detector::meta_server_failure_detector(meta_service* svc)
{
    _is_primary = false;
    _svc = svc;

    const meta_options& opt = _svc->get_meta_options();

    // create lock service
    _lock_svc = dsn::utils::factory_store<dist::distributed_lock_service>::create(
        opt.distributed_lock_service_type.c_str(),
        PROVIDER_TYPE_MAIN
        );
    error_code err = _lock_svc->initialize(opt.distributed_lock_service_args);
    dassert(err == ERR_OK, "init distributed_lock_service failed, err = %s", err.to_string());

    _primary_lock_id = "dsn.meta.server.leader";
}

meta_server_failure_detector::~meta_server_failure_detector()
{
    if (_lock_grant_task)
        _lock_grant_task->cancel(true);
    if (_lock_expire_task)
        _lock_expire_task->cancel(true);
    if ( _lock_svc )
    {
        _lock_svc->finalize();
        delete _lock_svc;
    }
}

void meta_server_failure_detector::on_worker_disconnected(const std::vector<rpc_address>& nodes)
{
    _svc->set_node_state(nodes, false);
}

void meta_server_failure_detector::on_worker_connected(rpc_address node)
{
    _svc->set_node_state( std::vector<rpc_address>{ node }, true);
}

DEFINE_TASK_CODE(LPC_META_SERVER_LEADER_LOCK_CALLBACK, TASK_PRIORITY_COMMON, fd::THREAD_POOL_FD)
void meta_server_failure_detector::acquire_leader_lock()
{
    //
    // try to get the leader lock until it is done
    //
    dsn::dist::distributed_lock_service::lock_options opt = {true, true};
    std::string local_owner_id;
    while (true)
    {
        error_code err;
        auto tasks = _lock_svc->lock(
            _primary_lock_id,
            primary_address().to_std_string(), 
            // lock granted
            LPC_META_SERVER_LEADER_LOCK_CALLBACK,
            [this, &err, &local_owner_id](error_code ec, const std::string& owner, uint64_t version)
            {
                err = ec;
                local_owner_id = owner;
            },

            // lease expire
            LPC_META_SERVER_LEADER_LOCK_CALLBACK,
            [this](error_code ec, const std::string& owner, uint64_t version)
            {
                // let's take the easy way right now
                dsn_exit(0);
            },
            opt
        );

        _lock_grant_task = tasks.first;
        _lock_expire_task = tasks.second;

        _lock_grant_task->wait();
        if (err == ERR_OK)
        {
            rpc_address addr;
            if (addr.from_string_ipv4(local_owner_id.c_str()))
            {
                dassert(primary_address() == addr, "");
                set_primary(addr);
                break;
            }
        }
    }
}

void meta_server_failure_detector::sync_node_state_and_start_service()
{
    /*
     * we do need the failure_detector::_lock to protect,
     * because we want to keep the states of server_state::_nodes
     * and meta_service::{alive_set,dead_set} consistent
     */
    zauto_lock l(failure_detector::_lock);

    std::set<rpc_address> nodes;
    _svc->prepare_service_starting();
    _svc->get_node_state(nodes, true);
    for(auto& node: nodes) {
        // a worker may have been dead in the fd, so we must reactive it
        unregister_worker(node);
        register_worker(node, true);
    }

    //now nodes in server_state and in fd are in consistent state
    _svc->service_starting();
}

void meta_server_failure_detector::set_primary(rpc_address primary)
{
    /*
    * we don't do register worker things in set_primary
    * as only nodes sync from meta_state_service are useful, 
    * but currently, we haven't do sync yet
    */
    bool old = _is_primary;
    {
        utils::auto_lock<zlock> l(_primary_address_lock);
        _primary_address = primary;
        _is_primary = (primary == primary_address());
        if (_is_primary)
        {
            _election_moment = dsn_now_ms();
        }
    }

    if (old && !_is_primary)
    {
        clear_workers();
    }
}


void meta_server_failure_detector::on_ping(const fd::beacon_msg& beacon, rpc_replier<fd::beacon_ack>& reply)
{
    fd::beacon_ack ack;
    ack.time = beacon.time;
    ack.this_node = beacon.to_addr;
    ack.allowed = true;

    if ( !is_primary() )
    {
        ack.is_master = false;
        ack.primary_node = get_primary();
    }
    else 
    {
        ack.is_master = true;
        ack.primary_node = beacon.to_addr;
        failure_detector::on_ping_internal(beacon, ack);
    }

    dinfo("on_ping, is_master(%s), from_node(%s), this_node(%s), primary_node(%s)", ack.is_master?"true":"false",
          beacon.from_addr.to_string(), ack.this_node.to_string(), ack.primary_node.to_string());
    reply(ack);
}

/*the following functions are only for test*/
meta_server_failure_detector::meta_server_failure_detector(rpc_address leader_address, bool is_myself_leader)
{
    _lock_svc = nullptr;
    _primary_address = leader_address;
    _is_primary = is_myself_leader;
}

void meta_server_failure_detector::set_leader_for_test(rpc_address leader_address, bool is_myself_leader)
{
    utils::auto_lock<zlock> l(_primary_address_lock);
    _primary_address = leader_address;
    _is_primary = is_myself_leader;
}

}}
