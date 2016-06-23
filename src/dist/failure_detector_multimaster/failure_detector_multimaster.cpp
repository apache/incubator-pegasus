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

#include <dsn/dist/failure_detector_multimaster.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "dist.FD.multimaster"

namespace dsn { 
    namespace dist {

        slave_failure_detector_with_multimaster::slave_failure_detector_with_multimaster(
            std::vector< ::dsn::rpc_address>& meta_servers,
            std::function<void()>&& master_disconnected_callback,
            std::function<void()>&& master_connected_callback
            )
        {
            _meta_servers.assign_group(dsn_group_build("meta-servers"));
            for (auto& s : meta_servers)
            {
                dsn_group_add(_meta_servers.group_handle(), s.c_addr());
            }

            dsn_group_set_leader(_meta_servers.group_handle(),
                meta_servers[random32(0, (uint32_t)meta_servers.size() - 1)].c_addr());
            // ATTENTION: here we disable dsn_group_set_update_leader_automatically to avoid
            // failure detecting logic is affected by rpc failure or rpc forwarding.
            dsn_group_set_update_leader_automatically(_meta_servers.group_handle(), false);

            _master_disconnected_callback = std::move(master_disconnected_callback);
            _master_connected_callback = std::move(master_connected_callback);
        }

        slave_failure_detector_with_multimaster::~slave_failure_detector_with_multimaster(void)
        {
            dsn_group_destroy(_meta_servers.group_handle());
        }

        void slave_failure_detector_with_multimaster::set_leader_for_test(rpc_address meta)
        {
            dsn_group_set_leader(_meta_servers.group_handle(), meta.c_addr());
        }

        void slave_failure_detector_with_multimaster::end_ping(::dsn::error_code err, const fd::beacon_ack& ack, void*)
        {
            dinfo("end ping result, error[%s], time[%" PRId64 "], ack.this_node[%s], ack.primary_node[%s], ack.is_master[%s], ack.allowed[%s]",
                err.to_string(), ack.time, ack.this_node.to_string(), ack.primary_node.to_string(),
                ack.is_master ? "true" : "false", ack.allowed ? "true" : "false");

            service::zauto_lock l(failure_detector::_lock);
            if (!failure_detector::end_ping_internal(err, ack))
                return;

            dassert(ack.this_node == dsn_group_get_leader(_meta_servers.group_handle()),
                "ack.this_node[%s] vs meta_servers.leader[%s]",
                ack.this_node.to_string(), dsn_address_to_string(dsn_group_get_leader(_meta_servers.group_handle())));

            if (ERR_OK != err) 
            {
                rpc_address next = dsn_group_next(_meta_servers.group_handle(), ack.this_node.c_addr());
                if (next != ack.this_node) {
                    dsn_group_set_leader(_meta_servers.group_handle(), next.c_addr());
                    // do not start next send_beacon() immediately to avoid send rpc too frequently
                    switch_master(ack.this_node, next, 1000);
                }
            }
            else
            {
                if (ack.is_master) {
                    //do nothing
                }
                else if (ack.primary_node.is_invalid()) {
                    rpc_address next = dsn_group_next(_meta_servers.group_handle(), ack.this_node.c_addr());
                    if (next != ack.this_node) {
                        dsn_group_set_leader(_meta_servers.group_handle(), next.c_addr());
                        // do not start next send_beacon() immediately to avoid send rpc too frequently
                        switch_master(ack.this_node, next, 1000);
                    }
                }
                else {
                    dsn_group_set_leader(_meta_servers.group_handle(), ack.primary_node.c_addr());
                    // start next send_beacon() immediately because the leader is possibly right.
                    switch_master(ack.this_node, ack.primary_node, 0);
                }
            }
        }

        // client side
        void slave_failure_detector_with_multimaster::on_master_disconnected(const std::vector< ::dsn::rpc_address>& nodes)
        {
            bool primary_disconnected = false;
            rpc_address leader = dsn_group_get_leader(_meta_servers.group_handle());
            for (auto it = nodes.begin(); it != nodes.end(); ++it)
            {
                if (leader == *it)
                    primary_disconnected = true;
            }

            if (primary_disconnected)
            {
                _master_disconnected_callback();
            }
        }

        void slave_failure_detector_with_multimaster::on_master_connected(::dsn::rpc_address node)
        {
            /*
            * well, this is called in on_ping_internal, which is called by rep::end_ping.
            * So this function is called in the lock context of fd::_lock
            */
            bool is_primary = false;
            {
                is_primary = dsn_group_is_leader(_meta_servers.group_handle(), node.c_addr());
            }

            if (is_primary)
            {
                _master_connected_callback();
            }
        }        
    }
} // end namespace

