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

#include "meta/meta_server_failure_detector.h"

#include <chrono>
#include <thread>
#include <utility>

#include <string_view>
#include "fd_types.h"
#include "meta/meta_options.h"
#include "meta/meta_service.h"
#include "runtime/app_model.h"
#include "rpc/rpc_address.h"
#include "runtime/serverlet.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/distributed_lock_service.h"
#include "utils/error_code.h"
#include "utils/factory_store.h"
#include "utils/fail_point.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"

DSN_DEFINE_int32(meta_server,
                 max_succssive_unstable_restart,
                 5,
                 "The maximum number of times a ReplicaServer can be disconnected. If the number "
                 "of a ReplicaServer disconnects exceeds this threshold, MetaServer will add it to "
                 "the blacklist to avoid instability caused by frequent reconnection");
DSN_DEFINE_uint64(meta_server,
                  stable_rs_min_running_seconds,
                  600,
                  "The time threshold for determining whether a ReplicaServer is running stably");
DSN_DEFINE_string(meta_server,
                  distributed_lock_service_type,
#ifdef MOCK_TEST
                  "distributed_lock_service_simple",
#else
                  "distributed_lock_service_zookeeper",
#endif
                  "The implementation class of distributed lock service");

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
        FLAGS_distributed_lock_service_type, PROVIDER_TYPE_MAIN);
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

void meta_server_failure_detector::on_worker_disconnected(const std::vector<host_port> &nodes)
{
    _svc->set_node_state(nodes, false);
}

void meta_server_failure_detector::on_worker_connected(const host_port &node)
{
    _svc->set_node_state({node}, true);
}

bool meta_server_failure_detector::get_leader(host_port *leader)
{
    FAIL_POINT_INJECT_F("meta_server_failure_detector_get_leader", [leader](std::string_view str) {
        /// the format of str is : true#{ip}:{port} or false#{ip}:{port}
        auto pos = str.find("#");
        // get leader host_port
        auto addr_part = str.substr(pos + 1, str.length() - pos - 1);
        *leader = host_port::from_string(addr_part.data());
        CHECK(*leader, "parse {} to rpc_address failed", addr_part);

        // get the return value which implies whether the current node is primary or not
        bool is_leader = true;
        auto is_leader_part = str.substr(0, pos);
        if (!dsn::buf2bool(is_leader_part, is_leader)) {
            CHECK(false, "parse {} to bool failed", is_leader_part);
        }
        return is_leader;
    });

    dsn::host_port holder;
    if (leader == nullptr) {
        leader = &holder;
    }

    if (_is_leader.load()) {
        *leader = dsn_primary_host_port();
        return true;
    }

    if (_lock_svc == nullptr) {
        leader->reset();
        return false;
    }

    std::string lock_owner;
    uint64_t version;
    error_code err = _lock_svc->query_cache(_primary_lock_id, lock_owner, version);
    if (err != dsn::ERR_OK) {
        LOG_WARNING("query leader from cache got error({})", err);
        leader->reset();
        return false;
    }

    *leader = host_port::from_string(lock_owner);
    if (!(*leader)) {
        leader->reset();
        return false;
    }

    return (*leader) == dsn_primary_host_port();
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
            dsn_primary_host_port().to_string(),
            // lock granted
            LPC_META_SERVER_LEADER_LOCK_CALLBACK,
            [this, &err](error_code ec, const std::string &owner, uint64_t version) {
                LOG_INFO("leader lock granted callback: err({}), owner({}), version({})",
                         ec,
                         owner,
                         version);
                err = ec;
                if (err == dsn::ERR_OK) {
                    leader_initialize(owner);
                }
            },

            // lease expire
            LPC_META_SERVER_LEADER_LOCK_CALLBACK,
            [](error_code ec, const std::string &owner, uint64_t version) {
                LOG_ERROR("leader lock expired callback: err({}), owner({}), version({})",
                          ec,
                          owner,
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

void meta_server_failure_detector::reset_stability_stat(const host_port &node)
{
    zauto_lock l(_map_lock);
    auto iter = _stablity.find(node);
    if (iter == _stablity.end())
        return;
    else {
        LOG_INFO("old stability stat: node({}), start_time({}), unstable_count({}), will reset "
                 "unstable count to 0",
                 node,
                 iter->second.last_start_time_ms,
                 iter->second.unstable_restart_count);
        iter->second.unstable_restart_count = 0;
    }
}

void meta_server_failure_detector::leader_initialize(const std::string &lock_service_owner)
{
    const auto hp = dsn::host_port::from_string(lock_service_owner);
    CHECK(hp, "parse {} to host_port failed", lock_service_owner);
    CHECK_EQ_MSG(hp, dsn_primary_host_port(), "acquire leader return success, but owner not match");
    _is_leader.store(true);
    _election_moment.store(dsn_now_ms());
}

bool meta_server_failure_detector::update_stability_stat(const fd::beacon_msg &beacon)
{
    zauto_lock l(_map_lock);

    host_port hp_from_node;
    GET_HOST_PORT(beacon, from_node, hp_from_node);

    auto iter = _stablity.find(hp_from_node);
    if (iter == _stablity.end()) {
        _stablity.emplace(hp_from_node, worker_stability{beacon.start_time, 0});
        return true;
    } else {
        worker_stability &w = iter->second;
        if (beacon.start_time == w.last_start_time_ms) {
            LOG_DEBUG("{} isn't restarted, last_start_time({})",
                      FMT_HOST_PORT_AND_IP(beacon, from_node),
                      w.last_start_time_ms);
            if (dsn_now_ms() - w.last_start_time_ms >= FLAGS_stable_rs_min_running_seconds * 1000 &&
                w.unstable_restart_count > 0) {
                LOG_INFO("{} has stably run for a while, reset it's unstable count({}) to 0",
                         FMT_HOST_PORT_AND_IP(beacon, from_node),
                         w.unstable_restart_count);
                w.unstable_restart_count = 0;
            }
        } else if (beacon.start_time > w.last_start_time_ms) {
            LOG_INFO("check {} restarted, last_time({}), this_time({})",
                     FMT_HOST_PORT_AND_IP(beacon, from_node),
                     w.last_start_time_ms,
                     beacon.start_time);
            if (beacon.start_time - w.last_start_time_ms <
                FLAGS_stable_rs_min_running_seconds * 1000) {
                w.unstable_restart_count++;
                LOG_WARNING("{} encounter an unstable restart, total_count({})",
                            FMT_HOST_PORT_AND_IP(beacon, from_node),
                            w.unstable_restart_count);
            } else if (w.unstable_restart_count > 0) {
                LOG_INFO("{} restart in {} ms after last restart, may recover ok, reset "
                         "it's unstable count({}) to 0",
                         FMT_HOST_PORT_AND_IP(beacon, from_node),
                         beacon.start_time - w.last_start_time_ms,
                         w.unstable_restart_count);
                w.unstable_restart_count = 0;
            }

            w.last_start_time_ms = beacon.start_time;
        } else {
            LOG_WARNING("{}: possible encounter a staled message, ignore it",
                        FMT_HOST_PORT_AND_IP(beacon, from_node));
        }
        return w.unstable_restart_count < FLAGS_max_succssive_unstable_restart;
    }
}

void meta_server_failure_detector::on_ping(const fd::beacon_msg &beacon,
                                           rpc_replier<fd::beacon_ack> &reply)
{
    if (beacon.__isset.start_time && !update_stability_stat(beacon)) {
        LOG_WARNING("{} is unstable, don't response to it's beacon",
                    FMT_HOST_PORT_AND_IP(beacon, from_node));
        return;
    }

    fd::beacon_ack ack;
    ack.time = beacon.time;
    SET_OBJ_IP_AND_HOST_PORT(ack, this_node, beacon, to_node);
    ack.allowed = true;

    dsn::host_port leader;
    if (!get_leader(&leader)) {
        ack.is_master = false;
        SET_IP_AND_HOST_PORT_BY_DNS(ack, primary_node, leader);
    } else {
        ack.is_master = true;
        SET_OBJ_IP_AND_HOST_PORT(ack, primary_node, beacon, to_node);
        failure_detector::on_ping_internal(beacon, ack);
    }

    LOG_INFO("on_ping, beacon send time[{}], is_master({}), from_node({}), this_node({}), "
             "primary_node({})",
             ack.time,
             ack.is_master ? "true" : "false",
             FMT_HOST_PORT_AND_IP(beacon, from_node),
             FMT_HOST_PORT_AND_IP(beacon, to_node),
             FMT_HOST_PORT_AND_IP(ack, primary_node));

    reply(ack);
}

/*the following functions are only for test*/
meta_server_failure_detector::meta_server_failure_detector(const host_port &leader_host_port,
                                                           bool is_myself_leader)
{
    LOG_INFO("set {} as leader", leader_host_port);
    _lock_svc = nullptr;
    _is_leader.store(is_myself_leader);
}

void meta_server_failure_detector::set_leader_for_test(const host_port &leader_host_port,
                                                       bool is_myself_leader)
{
    LOG_INFO("set {} as leader", leader_host_port);
    _is_leader.store(is_myself_leader);
}

meta_server_failure_detector::stability_map *
meta_server_failure_detector::get_stability_map_for_test()
{
    return &_stablity;
}
} // namespace replication
} // namespace dsn
