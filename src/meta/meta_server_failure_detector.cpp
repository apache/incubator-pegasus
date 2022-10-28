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

#include "utils/factory_store.h"
#include "utils/fail_point.h"
#include "utils/fmt_logging.h"
#include "meta_server_failure_detector.h"
#include "server_state.h"
#include "meta_service.h"
#include "meta_options.h"

#include <chrono>
#include <thread>

namespace dsn {
namespace replication {

meta_server_failure_detector::meta_server_failure_detector(meta_service *svc)
    : _svc(svc),
      _lock_svc(nullptr),
      _primary_lock_id("dsn.meta.server.leader"),
      _is_leader(false),
      _election_moment(0)
{
    _fd_opts = &(svc->get_meta_options()._fd_opts);
    _lock_svc = dsn::utils::factory_store<dist::distributed_lock_service>::create(
        _fd_opts->distributed_lock_service_type.c_str(), PROVIDER_TYPE_MAIN);
    error_code err = _lock_svc->initialize(_fd_opts->distributed_lock_service_args);
    CHECK_EQ_MSG(err, ERR_OK, "init distributed_lock_service failed");
}

meta_server_failure_detector::~meta_server_failure_detector()
{
    if (_lock_grant_task)
        _lock_grant_task->cancel(true);
    if (_lock_expire_task)
        _lock_expire_task->cancel(true);
    if (_lock_svc) {
        _lock_svc->finalize();
        delete _lock_svc;
    }
}

void meta_server_failure_detector::on_worker_disconnected(const std::vector<rpc_address> &nodes)
{
    _svc->set_node_state(nodes, false);
}

void meta_server_failure_detector::on_worker_connected(rpc_address node)
{
    _svc->set_node_state(std::vector<rpc_address>{node}, true);
}

bool meta_server_failure_detector::get_leader(rpc_address *leader)
{
    FAIL_POINT_INJECT_F("meta_server_failure_detector_get_leader", [leader](dsn::string_view str) {
        /// the format of str is : true#{ip}:{port} or false#{ip}:{port}
        auto pos = str.find("#");
        // get leader addr
        auto addr_part = str.substr(pos + 1, str.length() - pos - 1);
        if (!leader->from_string_ipv4(addr_part.data())) {
            CHECK(false, "parse {} to rpc_address failed", addr_part);
        }

        // get the return value which implies whether the current node is primary or not
        bool is_leader = true;
        auto is_leader_part = str.substr(0, pos);
        if (!dsn::buf2bool(is_leader_part, is_leader)) {
            CHECK(false, "parse {} to bool failed", is_leader_part);
        }
        return is_leader;
    });

    dsn::rpc_address holder;
    if (leader == nullptr) {
        leader = &holder;
    }

    if (_is_leader.load()) {
        *leader = dsn_primary_address();
        return true;
    } else if (_lock_svc == nullptr) {
        leader->set_invalid();
        return false;
    } else {
        std::string lock_owner;
        uint64_t version;
        error_code err = _lock_svc->query_cache(_primary_lock_id, lock_owner, version);
        if (err == dsn::ERR_OK && leader->from_string_ipv4(lock_owner.c_str())) {
            return (*leader) == dsn_primary_address();
        } else {
            LOG_WARNING("query leader from cache got error(%s)", err.to_string());
            leader->set_invalid();
            return false;
        }
    }
}

DEFINE_TASK_CODE(LPC_META_SERVER_LEADER_LOCK_CALLBACK, TASK_PRIORITY_COMMON, fd::THREAD_POOL_FD)
void meta_server_failure_detector::acquire_leader_lock()
{
    //
    // try to get the leader lock until it is done
    //
    dsn::dist::distributed_lock_service::lock_options opt = {true, true};
    while (true) {
        error_code err;
        auto tasks = _lock_svc->lock(
            _primary_lock_id,
            dsn_primary_address().to_std_string(),
            // lock granted
            LPC_META_SERVER_LEADER_LOCK_CALLBACK,
            [this, &err](error_code ec, const std::string &owner, uint64_t version) {
                LOG_INFO("leader lock granted callback: err(%s), owner(%s), version(%llu)",
                         ec.to_string(),
                         owner.c_str(),
                         version);
                err = ec;
                if (err == dsn::ERR_OK) {
                    leader_initialize(owner);
                }
            },

            // lease expire
            LPC_META_SERVER_LEADER_LOCK_CALLBACK,
            [](error_code ec, const std::string &owner, uint64_t version) {
                LOG_ERROR("leader lock expired callback: err(%s), owner(%s), version(%llu)",
                          ec.to_string(),
                          owner.c_str(),
                          version);
                // let's take the easy way right now
                dsn_exit(0);
            },
            opt);

        _lock_grant_task = tasks.first;
        _lock_expire_task = tasks.second;

        _lock_grant_task->wait();
        if (err == ERR_OK) {
            break;
        } else {
            // sleep for 1 second before retry
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void meta_server_failure_detector::reset_stability_stat(const rpc_address &node)
{
    zauto_lock l(_map_lock);
    auto iter = _stablity.find(node);
    if (iter == _stablity.end())
        return;
    else {
        LOG_INFO("old stability stat: node(%s), start_time(%lld), unstable_count(%d), will reset "
                 "unstable count to 0",
                 node.to_string(),
                 iter->second.last_start_time_ms,
                 iter->second.unstable_restart_count);
        iter->second.unstable_restart_count = 0;
    }
}

void meta_server_failure_detector::leader_initialize(const std::string &lock_service_owner)
{
    dsn::rpc_address addr;
    CHECK(addr.from_string_ipv4(lock_service_owner.c_str()),
          "parse {} to rpc_address failed",
          lock_service_owner);
    CHECK_EQ_MSG(addr, dsn_primary_address(), "acquire leader return success, but owner not match");
    _is_leader.store(true);
    _election_moment.store(dsn_now_ms());
}

bool meta_server_failure_detector::update_stability_stat(const fd::beacon_msg &beacon)
{
    zauto_lock l(_map_lock);
    auto iter = _stablity.find(beacon.from_addr);
    if (iter == _stablity.end()) {
        _stablity.emplace(beacon.from_addr, worker_stability{beacon.start_time, 0});
        return true;
    } else {
        worker_stability &w = iter->second;
        if (beacon.start_time == w.last_start_time_ms) {
            LOG_DEBUG("%s isn't restarted, last_start_time(%lld)",
                      beacon.from_addr.to_string(),
                      w.last_start_time_ms);
            if (dsn_now_ms() - w.last_start_time_ms >=
                    _fd_opts->stable_rs_min_running_seconds * 1000 &&
                w.unstable_restart_count > 0) {
                LOG_INFO("%s has stably run for a while, reset it's unstable count(%d) to 0",
                         beacon.from_addr.to_string(),
                         w.unstable_restart_count);
                w.unstable_restart_count = 0;
            }
        } else if (beacon.start_time > w.last_start_time_ms) {
            LOG_INFO("check %s restarted, last_time(%lld), this_time(%lld)",
                     beacon.from_addr.to_string(),
                     w.last_start_time_ms,
                     beacon.start_time);
            if (beacon.start_time - w.last_start_time_ms <
                _fd_opts->stable_rs_min_running_seconds * 1000) {
                w.unstable_restart_count++;
                LOG_WARNING("%s encounter an unstable restart, total_count(%d)",
                            beacon.from_addr.to_string(),
                            w.unstable_restart_count);
            } else if (w.unstable_restart_count > 0) {
                LOG_INFO("%s restart in %lld ms after last restart, may recover ok, reset "
                         "it's unstable count(%d) to 0",
                         beacon.from_addr.to_string(),
                         beacon.start_time - w.last_start_time_ms,
                         w.unstable_restart_count);
                w.unstable_restart_count = 0;
            }

            w.last_start_time_ms = beacon.start_time;
        } else {
            LOG_WARNING("%s: possible encounter a staled message, ignore it",
                        beacon.from_addr.to_string());
        }
        return w.unstable_restart_count < _fd_opts->max_succssive_unstable_restart;
    }
}

void meta_server_failure_detector::on_ping(const fd::beacon_msg &beacon,
                                           rpc_replier<fd::beacon_ack> &reply)
{
    fd::beacon_ack ack;
    ack.time = beacon.time;
    ack.this_node = beacon.to_addr;
    ack.allowed = true;

    if (beacon.__isset.start_time && !update_stability_stat(beacon)) {
        LOG_WARNING("%s is unstable, don't response to it's beacon", beacon.from_addr.to_string());
        return;
    }

    dsn::rpc_address leader;
    if (!get_leader(&leader)) {
        ack.is_master = false;
        ack.primary_node = leader;
    } else {
        ack.is_master = true;
        ack.primary_node = beacon.to_addr;
        failure_detector::on_ping_internal(beacon, ack);
    }

    LOG_INFO("on_ping, beacon send time[%ld], is_master(%s), from_node(%s), this_node(%s), "
             "primary_node(%s)",
             ack.time,
             ack.is_master ? "true" : "false",
             beacon.from_addr.to_string(),
             ack.this_node.to_string(),
             ack.primary_node.to_string());

    reply(ack);
}

/*the following functions are only for test*/
meta_server_failure_detector::meta_server_failure_detector(rpc_address leader_address,
                                                           bool is_myself_leader)
{
    LOG_INFO("set %s as leader", leader_address.to_string());
    _lock_svc = nullptr;
    _is_leader.store(is_myself_leader);
}

void meta_server_failure_detector::set_leader_for_test(rpc_address leader_address,
                                                       bool is_myself_leader)
{
    LOG_INFO("set %s as leader", leader_address.to_string());
    _is_leader.store(is_myself_leader);
}

meta_server_failure_detector::stability_map *
meta_server_failure_detector::get_stability_map_for_test()
{
    return &_stablity;
}
}
}
