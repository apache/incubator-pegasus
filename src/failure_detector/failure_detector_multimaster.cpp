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

#include <stdint.h>
#include <utility>

#include "failure_detector/failure_detector_multimaster.h"
#include "fd_types.h"
#include "runtime/rpc/group_address.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/error_code.h"
#include "utils/rand.h"

namespace dsn {
namespace dist {

slave_failure_detector_with_multimaster::slave_failure_detector_with_multimaster(
    std::vector<::dsn::rpc_address> &meta_servers,
    std::function<void()> &&master_disconnected_callback,
    std::function<void()> &&master_connected_callback)
{
    _meta_servers.assign_group("meta-servers");
    for (const auto &s : meta_servers) {
        if (!_meta_servers.group_address()->add(s)) {
            LOG_WARNING("duplicate adress {}", s);
        }
    }

    _meta_servers.group_address()->set_leader(
        meta_servers[rand::next_u32(0, (uint32_t)meta_servers.size() - 1)]);

    // ATTENTION: here we disable dsn_group_set_update_leader_automatically to avoid
    // failure detecting logic is affected by rpc failure or rpc forwarding.
    _meta_servers.group_address()->set_update_leader_automatically(false);

    _master_disconnected_callback = std::move(master_disconnected_callback);
    _master_connected_callback = std::move(master_connected_callback);
}

void slave_failure_detector_with_multimaster::set_leader_for_test(rpc_address meta)
{
    _meta_servers.group_address()->set_leader(meta);
}

void slave_failure_detector_with_multimaster::end_ping(::dsn::error_code err,
                                                       const fd::beacon_ack &ack,
                                                       void *)
{
    LOG_INFO("end ping result, error[{}], time[{}], ack.this_node[{}], ack.primary_node[{}], "
             "ack.is_master[{}], ack.allowed[{}]",
             err,
             ack.time,
             ack.this_node,
             ack.primary_node,
             ack.is_master ? "true" : "false",
             ack.allowed ? "true" : "false");

    zauto_lock l(failure_detector::_lock);
    if (!failure_detector::end_ping_internal(err, ack))
        return;

    CHECK_EQ(ack.this_node, _meta_servers.group_address()->leader());

    if (ERR_OK != err) {
        rpc_address next = _meta_servers.group_address()->next(ack.this_node);
        if (next != ack.this_node) {
            _meta_servers.group_address()->set_leader(next);
            // do not start next send_beacon() immediately to avoid send rpc too frequently
            switch_master(ack.this_node, next, 1000);
        }
    } else {
        if (ack.is_master) {
            // do nothing
        } else if (ack.primary_node.is_invalid()) {
            rpc_address next = _meta_servers.group_address()->next(ack.this_node);
            if (next != ack.this_node) {
                _meta_servers.group_address()->set_leader(next);
                // do not start next send_beacon() immediately to avoid send rpc too frequently
                switch_master(ack.this_node, next, 1000);
            }
        } else {
            _meta_servers.group_address()->set_leader(ack.primary_node);
            // start next send_beacon() immediately because the leader is possibly right.
            switch_master(ack.this_node, ack.primary_node, 0);
        }
    }
}

// client side
void slave_failure_detector_with_multimaster::on_master_disconnected(
    const std::vector<::dsn::rpc_address> &nodes)
{
    bool primary_disconnected = false;
    rpc_address leader = _meta_servers.group_address()->leader();
    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        if (leader == *it)
            primary_disconnected = true;
    }

    if (primary_disconnected) {
        _master_disconnected_callback();
    }
}

void slave_failure_detector_with_multimaster::on_master_connected(::dsn::rpc_address node)
{
    /*
    * well, this is called in on_ping_internal, which is called by rep::end_ping.
    * So this function is called in the lock context of fd::_lock
    */
    bool is_primary = (_meta_servers.group_address()->leader() == node);
    if (is_primary) {
        _master_connected_callback();
    }
}
}
} // end namespace
