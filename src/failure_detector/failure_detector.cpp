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

#include "failure_detector/failure_detector.h"

#include <chrono>
#include <ctime>

#include "utils/command_manager.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace fd {

failure_detector::failure_detector()
{
    dsn::threadpool_code pool = task_spec::get(LPC_BEACON_CHECK.code())->pool_code;
    task_spec::get(RPC_FD_FAILURE_DETECTOR_PING.code())->pool_code = pool;
    task_spec::get(RPC_FD_FAILURE_DETECTOR_PING_ACK.code())->pool_code = pool;

    _recent_beacon_fail_count.init_app_counter(
        "eon.failure_detector",
        "recent_beacon_fail_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "failure detector beacon fail count in the recent period");

    _is_started = false;
}

failure_detector::~failure_detector()
{
    stop();
    unregister_ctrl_commands();
}

void failure_detector::register_ctrl_commands()
{
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _get_allow_list = dsn::command_manager::instance().register_command(
            {"fd.allow_list"},
            "fd.allow_list",
            "show allow list of failure detector",
            [this](const std::vector<std::string> &args) { return get_allow_list(args); });
    });
}

void failure_detector::unregister_ctrl_commands() { _get_allow_list.reset(); }

error_code failure_detector::start(uint32_t check_interval_seconds,
                                   uint32_t beacon_interval_seconds,
                                   uint32_t lease_seconds,
                                   uint32_t grace_seconds,
                                   bool use_allow_list)
{
    _check_interval_milliseconds = check_interval_seconds * 1000;
    _beacon_interval_milliseconds = beacon_interval_seconds * 1000;
    // here we set beacon timeout less than beacon interval in order to switch master
    // immediately once the last beacon fails, to make failure detection more robust.
    _beacon_timeout_milliseconds = _beacon_interval_milliseconds * 2 / 3;
    _lease_milliseconds = lease_seconds * 1000;
    _grace_milliseconds = grace_seconds * 1000;

    _use_allow_list = use_allow_list;

    open_service();

    // start periodically check job
    _check_task = tasking::enqueue_timer(LPC_BEACON_CHECK,
                                         &_tracker,
                                         [this] { check_all_records(); },
                                         std::chrono::milliseconds(_check_interval_milliseconds),
                                         -1,
                                         std::chrono::milliseconds(_check_interval_milliseconds));

    _is_started = true;
    return ERR_OK;
}

void failure_detector::stop()
{
    _tracker.cancel_outstanding_tasks();

    zauto_lock l(_lock);
    if (!_is_started) {
        return;
    }
    _is_started = false;
    _masters.clear();
    _workers.clear();
}

void failure_detector::register_master(::dsn::rpc_address target)
{
    bool setup_timer = false;

    zauto_lock l(_lock);

    master_record record(target, dsn_now_ms());

    auto ret = _masters.insert(std::make_pair(target, record));
    if (ret.second) {
        LOG_DEBUG("register master[%s] successfully", target.to_string());
        setup_timer = true;
    } else {
        // active the beacon again in case previously local node is not in target's allow list
        if (ret.first->second.rejected) {
            ret.first->second.rejected = false;
            setup_timer = true;
        }
        LOG_DEBUG("master[%s] already registered", target.to_string());
    }

    if (setup_timer) {
        // delay the beacon slightly to make first beacon greater than the
        // last_beacon_send_time_with_ack
        ret.first->second.send_beacon_timer =
            tasking::enqueue_timer(LPC_BEACON_SEND,
                                   &_tracker,
                                   [this, target]() { this->send_beacon(target, dsn_now_ms()); },
                                   std::chrono::milliseconds(_beacon_interval_milliseconds),
                                   0,
                                   std::chrono::milliseconds(1));
    }
}

bool failure_detector::switch_master(::dsn::rpc_address from,
                                     ::dsn::rpc_address to,
                                     uint32_t delay_milliseconds)
{
    /* the caller of switch master shoud lock necessarily to protect _masters */
    auto it = _masters.find(from);
    auto it2 = _masters.find(to);
    if (it != _masters.end()) {
        if (it2 != _masters.end()) {
            LOG_WARNING("switch master failed as both are already registered, from[%s], to[%s]",
                        from.to_string(),
                        to.to_string());
            return false;
        }

        it->second.node = to;
        it->second.rejected = false;
        it->second.send_beacon_timer->cancel(true);
        it->second.send_beacon_timer =
            tasking::enqueue_timer(LPC_BEACON_SEND,
                                   &_tracker,
                                   [this, to]() { this->send_beacon(to, dsn_now_ms()); },
                                   std::chrono::milliseconds(_beacon_interval_milliseconds),
                                   0,
                                   std::chrono::milliseconds(delay_milliseconds));

        _masters.insert(std::make_pair(to, it->second));
        _masters.erase(from);

        LOG_INFO("switch master successfully, from[%s], to[%s]", from.to_string(), to.to_string());
    } else {
        LOG_WARNING("switch master failed as from node is not registered yet, from[%s], to[%s]",
                    from.to_string(),
                    to.to_string());
        return false;
    }
    return true;
}

bool failure_detector::is_time_greater_than(uint64_t ts, uint64_t base) { return ts > base; }

void failure_detector::report(::dsn::rpc_address node, bool is_master, bool is_connected)
{
    LOG_INFO("%s %sconnected: %s",
             is_master ? "master" : "worker",
             is_connected ? "" : "dis",
             node.to_string());
}

/*
                            |--- lease period ----|lease IsExpired, commit suicide
                 |--- lease period ---|
    worker: ---------------------------------------------------------------->
                 \    /     \    /      _\
             beacon ack  beacon ack       x (beacon deliver failed)
                  _\/        _\/
    master: ---------------------------------------------------------------->
                    |---- grace period ----|
                               |--- grace period ----| grace IsExpired, declare worker dead
*/

void failure_detector::check_all_records()
{
    if (!_is_started) {
        return;
    }

    std::vector<rpc_address> expire;

    {
        zauto_lock l(_lock);

        uint64_t now = dsn_now_ms();

        for (auto itr = _masters.begin(); itr != _masters.end(); itr++) {
            master_record &record = itr->second;

            /*
             * "Check interval" and "send beacon" are interleaved, so we must
             * test if "record will expire before next time we check all the records"
             * in order to guarantee the perfect fd
             */
            // we should ensure now is greater than record.last_send_time_for_beacon_with_ack
            // to aviod integer overflow
            if (record.is_alive &&
                is_time_greater_than(now, record.last_send_time_for_beacon_with_ack) &&
                now + _check_interval_milliseconds - record.last_send_time_for_beacon_with_ack >
                    _lease_milliseconds) {
                LOG_ERROR("master %s disconnected, now=%" PRId64 ", last_send_time=%" PRId64
                          ", now+check_interval-last_send_time=%" PRId64,
                          record.node.to_string(),
                          now,
                          record.last_send_time_for_beacon_with_ack,
                          now + _check_interval_milliseconds -
                              record.last_send_time_for_beacon_with_ack);

                expire.push_back(record.node);
                record.is_alive = false;

                report(record.node, true, false);
            }
        }

        /*
         * The disconnected event MUST be under the protection of the _lock
         * we must guarantee that the record.is_alive switch and the connect/disconnect
         * callbacks are ATOMIC as these callbacks usually have side effects.
         *
         * And you must be careful with these 2 virtual functions as it is very likely to have
         * nested locks
         */
        if (expire.size() > 0) {
            on_master_disconnected(expire);
        }
    }

    // process recv record, for server
    expire.clear();

    {
        zauto_lock l(_lock);

        uint64_t now = dsn_now_ms();

        for (auto itq = _workers.begin(); itq != _workers.end(); itq++) {
            worker_record &record = itq->second;

            // we should ensure now is greater than record.last_beacon_recv_time to aviod integer
            // overflow
            if (record.is_alive && is_time_greater_than(now, record.last_beacon_recv_time) &&
                now - record.last_beacon_recv_time > _grace_milliseconds) {
                LOG_ERROR("worker %s disconnected, now=%" PRId64 ", last_beacon_recv_time=%" PRId64
                          ", now-last_recv=%" PRId64,
                          record.node.to_string(),
                          now,
                          record.last_beacon_recv_time,
                          now - record.last_beacon_recv_time);

                expire.push_back(record.node);
                record.is_alive = false;

                report(record.node, false, false);
            }
        }
        /*
         * The worker disconnected event also need to be under protection of the _lock
         */
        if (expire.size() > 0) {
            on_worker_disconnected(expire);
        }
    }
}

void failure_detector::add_allow_list(::dsn::rpc_address node)
{
    zauto_lock l(_lock);
    _allow_list.insert(node);
}

bool failure_detector::remove_from_allow_list(::dsn::rpc_address node)
{
    zauto_lock l(_lock);
    return _allow_list.erase(node) > 0;
}

void failure_detector::set_allow_list(const std::vector<std::string> &replica_addrs)
{
    CHECK(!_is_started, "FD is already started, the allow list should really not be modified");

    std::vector<rpc_address> nodes;
    for (auto &addr : replica_addrs) {
        rpc_address node;
        if (!node.from_string_ipv4(addr.c_str())) {
            LOG_WARNING("replica_white_list has invalid ip %s, the allow list won't be modified",
                        addr.c_str());
            return;
        }
        nodes.push_back(node);
    }

    for (auto &node : nodes)
        add_allow_list(node);
}

std::string failure_detector::get_allow_list(const std::vector<std::string> &args) const
{
    if (!_is_started)
        return "error: FD is not started";

    std::stringstream oss;
    dsn::zauto_lock l(_lock);
    oss << "get ok: allow list " << (_use_allow_list ? "enabled. list: " : "disabled.");
    for (auto iter = _allow_list.begin(); iter != _allow_list.end(); ++iter) {
        if (iter != _allow_list.begin())
            oss << ",";
        oss << iter->to_string();
    }
    return oss.str();
}

void failure_detector::on_ping_internal(const beacon_msg &beacon, /*out*/ beacon_ack &ack)
{
    ack.time = beacon.time;
    ack.this_node = beacon.to_addr;
    ack.primary_node = dsn_primary_address();
    ack.is_master = true;
    ack.allowed = true;

    zauto_lock l(_lock);

    uint64_t now = dsn_now_ms();
    auto node = beacon.from_addr;

    worker_map::iterator itr = _workers.find(node);
    if (itr == _workers.end()) {
        // if is a new worker, check allow list first if need
        if (_use_allow_list && _allow_list.find(node) == _allow_list.end()) {
            LOG_WARNING("new worker[%s] is rejected", node.to_string());
            ack.allowed = false;
            return;
        }

        // create new entry for node
        worker_record record(node, now);
        record.is_alive = true;
        _workers.insert(std::make_pair(node, record));

        report(node, false, true);
        on_worker_connected(node);
    } else if (is_time_greater_than(now, itr->second.last_beacon_recv_time)) {
        // update last_beacon_recv_time
        itr->second.last_beacon_recv_time = now;

        LOG_INFO("master %s update last_beacon_recv_time=%" PRId64,
                 itr->second.node.to_string(),
                 itr->second.last_beacon_recv_time);

        if (itr->second.is_alive == false) {
            itr->second.is_alive = true;

            report(node, false, true);
            on_worker_connected(node);
        }
    } else {
        LOG_INFO("now[%" PRId64 "] <= last_recv_time[%" PRId64 "]",
                 now,
                 itr->second.last_beacon_recv_time);
    }
}

void failure_detector::on_ping(const beacon_msg &beacon, ::dsn::rpc_replier<beacon_ack> &reply)
{
    beacon_ack ack;
    on_ping_internal(beacon, ack);
    reply(ack);
}

void failure_detector::end_ping(::dsn::error_code err, const beacon_ack &ack, void *)
{
    end_ping_internal(err, ack);
}

bool failure_detector::end_ping_internal(::dsn::error_code err, const beacon_ack &ack)
{
    /*
     * the caller of the end_ping_internal should lock necessarily!!!
     */
    uint64_t beacon_send_time = ack.time;
    auto node = ack.this_node;

    if (err != ERR_OK) {
        LOG_WARNING("ping master(%s) failed, timeout_ms = %u, err = %s",
                    node.to_string(),
                    _beacon_timeout_milliseconds,
                    err.to_string());
        _recent_beacon_fail_count->increment();
    }

    master_map::iterator itr = _masters.find(node);

    if (itr == _masters.end()) {
        LOG_WARNING("received beacon ack without corresponding master, ignore it, "
                    "remote_master[%s], local_worker[%s]",
                    node.to_string(),
                    dsn_primary_address().to_string());
        return false;
    }

    master_record &record = itr->second;
    if (!ack.allowed) {
        LOG_WARNING("worker rejected, stop sending beacon message, "
                    "remote_master[%s], local_worker[%s]",
                    node.to_string(),
                    dsn_primary_address().to_string());
        record.rejected = true;
        record.send_beacon_timer->cancel(true);
        return false;
    }

    if (!is_time_greater_than(beacon_send_time, record.last_send_time_for_beacon_with_ack)) {
        // out-dated beacon acks, do nothing
        LOG_INFO("ignore out dated beacon acks, send_time(%lld), last_beacon(%lld)",
                 beacon_send_time,
                 record.last_send_time_for_beacon_with_ack);
        return false;
    }

    // now the ack is applicable
    if (err != ERR_OK) {
        return true;
    }

    // if ack is not from master meta, worker should not update its last send time
    if (!ack.is_master) {
        LOG_WARNING("node[%s] is not master, ack.primary_node[%s] is real master",
                    node.to_string(),
                    ack.primary_node.to_string());
        return true;
    }

    // update last_send_time_for_beacon_with_ack
    record.last_send_time_for_beacon_with_ack = beacon_send_time;
    record.rejected = false;

    LOG_INFO("worker %s send beacon succeed, update last_send_time=%" PRId64,
             record.node.to_string(),
             record.last_send_time_for_beacon_with_ack);

    uint64_t now = dsn_now_ms();
    // we should ensure now is greater than record.last_beacon_recv_time to aviod integer overflow
    if (!record.is_alive && is_time_greater_than(now, record.last_send_time_for_beacon_with_ack) &&
        now - record.last_send_time_for_beacon_with_ack <= _lease_milliseconds) {
        // report master connected
        report(node, true, true);
        itr->second.is_alive = true;
        on_master_connected(node);
    }

    return true;
}

bool failure_detector::unregister_master(::dsn::rpc_address node)
{
    zauto_lock l(_lock);
    auto it = _masters.find(node);

    if (it != _masters.end()) {
        it->second.send_beacon_timer->cancel(true);
        _masters.erase(it);
        LOG_INFO("unregister master[%s] successfully", node.to_string());
        return true;
    } else {
        LOG_INFO("unregister master[%s] failed, cannot find it in FD", node.to_string());
        return false;
    }
}

bool failure_detector::is_master_connected(::dsn::rpc_address node) const
{
    zauto_lock l(_lock);
    auto it = _masters.find(node);
    if (it != _masters.end())
        return it->second.is_alive;
    else
        return false;
}

void failure_detector::register_worker(::dsn::rpc_address target, bool is_connected)
{
    /*
     * callers should use the fd::_lock necessarily
     */
    worker_record record(target, dsn_now_ms());
    record.is_alive = is_connected ? true : false;

    auto ret = _workers.insert(std::make_pair(target, record));
    if (ret.second) {
        LOG_DEBUG("register worker[%s] successfully", target.to_string());
    } else {
        LOG_DEBUG("worker[%s] already registered", target.to_string());
    }
}

bool failure_detector::unregister_worker(::dsn::rpc_address node)
{
    /*
     * callers should use the fd::_lock necessarily
     */
    bool ret;

    size_t count = _workers.erase(node);

    if (count == 0) {
        ret = false;
    } else {
        ret = true;
    }

    LOG_DEBUG("unregister worker[%s] successfully, removed entry count is %u",
              node.to_string(),
              (uint32_t)count);

    return ret;
}

void failure_detector::clear_workers()
{
    zauto_lock l(_lock);
    _workers.clear();
}

bool failure_detector::is_worker_connected(::dsn::rpc_address node) const
{
    zauto_lock l(_lock);
    auto it = _workers.find(node);
    if (it != _workers.end())
        return it->second.is_alive;
    else
        return false;
}

void failure_detector::send_beacon(::dsn::rpc_address target, uint64_t time)
{
    beacon_msg beacon;
    beacon.time = time;
    beacon.from_addr = dsn_primary_address();
    beacon.to_addr = target;
    beacon.__set_start_time(static_cast<int64_t>(dsn::utils::process_start_millis()));

    LOG_INFO("send ping message, from[%s], to[%s], time[%" PRId64 "]",
             beacon.from_addr.to_string(),
             beacon.to_addr.to_string(),
             time);

    ::dsn::rpc::call(target,
                     RPC_FD_FAILURE_DETECTOR_PING,
                     beacon,
                     &_tracker,
                     [=](error_code err, beacon_ack &&resp) {
                         if (err != ::dsn::ERR_OK) {
                             beacon_ack ack;
                             ack.time = beacon.time;
                             ack.this_node = beacon.to_addr;
                             ack.primary_node.set_invalid();
                             ack.is_master = false;
                             ack.allowed = true;
                             end_ping(err, ack, nullptr);
                         } else {
                             end_ping(err, std::move(resp), nullptr);
                         }
                     },
                     std::chrono::milliseconds(_beacon_timeout_milliseconds));
}
}
} // end namespace
