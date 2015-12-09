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

#include "replication_failure_detector.h"
#include "replica_stub.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.FD"

namespace dsn { namespace replication {

replication_failure_detector::replication_failure_detector(
    replica_stub* stub, std::vector< ::dsn::rpc_address>& meta_servers)
{
    _meta_servers.assign_group(dsn_group_build("meta.servers"));
    _stub = stub;
    for (auto& s : meta_servers)
    {
        dsn_group_add(_meta_servers.group_handle(), s.c_addr());
    }

    dsn_group_set_leader(_meta_servers.group_handle(),
        meta_servers[random32(0, (uint32_t)meta_servers.size() - 1)].c_addr());
}

replication_failure_detector::~replication_failure_detector(void)
{
    dsn_group_destroy(_meta_servers.group_handle());
}

void replication_failure_detector::end_ping(::dsn::error_code err, const fd::beacon_ack& ack, void* context)
{
    dinfo("end ping result, error[%s], ack.this_node[%s], ack.primary_node[%s], ack.is_master[%s], ack.allowed[%s]",
          err.to_string(), ack.this_node.to_string(), ack.primary_node.to_string(),
          ack.is_master ? "true" : "false", ack.allowed ? "true" : "false");

    if (!failure_detector::end_ping_internal(err, ack))
    {
        // not applicable
        return;
    }

    {
        zauto_lock l(_meta_lock);

        if (dsn_group_is_leader(_meta_servers.group_handle(), ack.this_node.c_addr()))
        {
            if (err != ERR_OK)
            {
                // ping failed, switch master
                rpc_address node = dsn_group_next(_meta_servers.group_handle(), ack.this_node.c_addr());
                if (ack.this_node != node)
                {
                    switch_master(ack.this_node, node);
                }
            }
            else if (ack.is_master == false)
            {
                // switch master
                if (!ack.primary_node.is_invalid()
                        && !dsn_group_is_leader(_meta_servers.group_handle(), ack.primary_node.c_addr()))
                {
                    switch_master(ack.this_node, ack.primary_node);
                }
            }
            else
            {
            }
        }
        else // ack.this_node is not leader
        {
            if (err != ERR_OK)
            {
                // do nothing
            }
            else if (ack.is_master == false)
            {
                // switch master
                if (!ack.primary_node.is_invalid()
                        && !dsn_group_is_leader(_meta_servers.group_handle(), ack.primary_node.c_addr()))
                {
                    switch_master(ack.this_node, ack.primary_node);
                }
            }
            else
            {
                ddebug("update meta server leader to [%s]", ack.this_node.to_string());
                dsn_group_set_leader(_meta_servers.group_handle(), ack.this_node.c_addr());
            }
        }
    }
}

// client side
void replication_failure_detector::on_master_disconnected( const std::vector< ::dsn::rpc_address>& nodes )
{
    bool primaryDisconnected = false;
    rpc_address leader = dsn_group_get_leader(_meta_servers.group_handle());

    {
        zauto_lock l(_meta_lock);
        for (auto it = nodes.begin(); it != nodes.end(); it++)
        {
            if (leader == *it)
                primaryDisconnected = true;
        }
    }

    if (primaryDisconnected)
    {
        _stub->on_meta_server_disconnected();
    }
}

void replication_failure_detector::on_master_connected(::dsn::rpc_address node)
{
    bool is_primary = false;

    {
        zauto_lock l(_meta_lock);
        is_primary = dsn_group_is_leader(_meta_servers.group_handle(), node.c_addr());
    }

    if (is_primary)
    {
        _stub->on_meta_server_connected();
    }
}

}} // end namespace

