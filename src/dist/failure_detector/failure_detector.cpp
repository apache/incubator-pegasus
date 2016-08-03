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


# include <dsn/dist/failure_detector.h>
# include <chrono>
# include <ctime>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "failure_detector"

using namespace ::dsn::service;

namespace dsn { 
namespace fd {

failure_detector::failure_detector()
{
    dsn_threadpool_code_t pool;
    dsn_task_code_query(LPC_BEACON_CHECK, nullptr, nullptr, &pool);
    dsn_task_code_set_threadpool(RPC_FD_FAILURE_DETECTOR_PING, pool);
    dsn_task_code_set_threadpool(RPC_FD_FAILURE_DETECTOR_PING_ACK, pool);

    _is_started = false;
}

error_code failure_detector::start(
    uint32_t check_interval_seconds, 
    uint32_t beacon_interval_seconds,
    uint32_t lease_seconds, 
    uint32_t grace_seconds, 
    bool use_allow_list)
{
    _check_interval_milliseconds = check_interval_seconds * 1000;
    _beacon_interval_milliseconds = beacon_interval_seconds * 1000;
    _lease_milliseconds = lease_seconds * 1000;
    _grace_milliseconds = grace_seconds * 1000;

    _use_allow_list   = use_allow_list;

    open_service();

    // start periodically check job
    _check_task = tasking::enqueue_timer(
        LPC_BEACON_CHECK,
        this,
        [this] {check_all_records();},
        std::chrono::milliseconds(_check_interval_milliseconds),
        -1,
        std::chrono::milliseconds(_check_interval_milliseconds));

    _is_started = true;
    return ERR_OK;
}

error_code failure_detector::stop()
{
    if ( _is_started == false )
    {
        return ERR_OK;
    }

    _is_started = false;

    close_service();

    {
        zauto_lock l(_lock);
        for (auto& m : _masters)
        {
            m.second.send_beacon_timer->cancel(true);
        }

        _masters.clear();
        _workers.clear();
    }

    if (_check_task != nullptr)
    {
        _check_task->cancel(true);
        _check_task = nullptr;
    }

    return ERR_OK;
}

void failure_detector::register_master(::dsn::rpc_address target)
{
    bool setup_timer = false;
    uint64_t now = now_ms();

    zauto_lock l(_lock);

    master_record record(target, now);

    auto ret = _masters.insert(std::make_pair(target, record));
    if (ret.second)
    {
        dinfo("register master[%s] successfully", target.to_string());
        setup_timer = true;
    }
    else
    {
        // active the beacon again in case previously local node is not in target's allow list
        if (ret.first->second.rejected)
        {
            ret.first->second.rejected = false;
            setup_timer = true;            
        }
        dinfo("master[%s] already registered", target.to_string());
    }

    if (setup_timer)
    {
        ret.first->second.send_beacon_timer = tasking::enqueue_timer(LPC_BEACON_SEND, this,
            [this, target]() 
            {
                this->send_beacon(target, now_ms());
            },
            std::chrono::milliseconds(_beacon_interval_milliseconds)
            );
    }
}

bool failure_detector::switch_master(::dsn::rpc_address from, ::dsn::rpc_address to, uint32_t delay_milliseconds)
{
    /* the caller of switch master shoud lock necessarily to protect _masters */
    auto it = _masters.find(from);
    auto it2 = _masters.find(to);
    if (it != _masters.end())
    {
        if (it2 != _masters.end())
        {
            dwarn("switch master failed as both are already registered, from[%s], to[%s]",
                  from.to_string(), to.to_string());
            return false;
        }

        it->second.node = to;
        it->second.rejected = false;
        it->second.send_beacon_timer->cancel(true);
        it->second.send_beacon_timer = tasking::enqueue_timer(LPC_BEACON_SEND, this,
            [this, to]()
            {
                this->send_beacon(to, now_ms());
            },
            std::chrono::milliseconds(_beacon_interval_milliseconds),
            0,
            std::chrono::milliseconds(delay_milliseconds)
            );

        _masters.insert(std::make_pair(to, it->second));
        _masters.erase(from);

        dinfo("switch master successfully, from[%s], to[%s]",
              from.to_string(), to.to_string());
    }
    else
    {
        dwarn("switch master failed as from node is not registered yet, from[%s], to[%s]",
              from.to_string(), to.to_string());
        return false;
    }
    return true;
}

bool failure_detector::is_time_greater_than(uint64_t ts, uint64_t base)
{
    uint64_t delta = ts - base;
    if (delta <= 24ULL*3600ULL*1000ULL)
        return true;
    else
        return false;
}

void failure_detector::report(::dsn::rpc_address node, bool is_master, bool is_connected)
{
    ddebug("%s[%s] %sconnected", is_master ? "master":"worker", node.to_string(), is_connected ? "" : "dis");
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
    if (!_is_started)
    {
        return;
    }

    std::vector< ::dsn::rpc_address> expire;
    uint64_t now =now_ms();

    {
        zauto_lock l(_lock);
        master_map::iterator itr = _masters.begin();
        for (; itr != _masters.end() ; itr++)
        {
            master_record& record = itr->second;

            /*
             * "Check interval" and "send beacon" are interleaved, so we must
             * test if "record will expire before next time we check all the records"
             * in order to guarantee the perfect fd
             */
            if (record.is_alive
                && now + _check_interval_milliseconds - record.last_send_time_for_beacon_with_ack > _lease_milliseconds)
            {
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
        if ( expire.size() > 0 )
        {
            on_master_disconnected(expire);
        }
    }


    // process recv record, for server
    expire.clear();
    now =now_ms();

    {
        zauto_lock l(_lock);
        worker_map::iterator itq = _workers.begin();
        for ( ; itq != _workers.end() ; itq++)
        {
            worker_record& record = itq->second;

            if (record.is_alive != false
                && now - record.last_beacon_recv_time > _grace_milliseconds)
            {
                expire.push_back(record.node);
                record.is_alive = false;

                report(record.node, false, false);
            }
        }
        /*
         * The worker disconnected event also need to be under protection of the _lock
         */
        if ( expire.size() > 0 )
        {
            on_worker_disconnected(expire);
        }
    }
}

void failure_detector::add_allow_list( ::dsn::rpc_address node)
{
    zauto_lock l(_lock);
    _allow_list.insert(node);
}

bool failure_detector::remove_from_allow_list( ::dsn::rpc_address node)
{
    zauto_lock l(_lock);
    return _allow_list.erase(node) > 0;
}

void failure_detector::on_ping_internal(const beacon_msg& beacon, /*out*/ beacon_ack& ack)
{
    ack.time = beacon.time;
    ack.this_node = beacon.to_addr;
    ack.primary_node = primary_address();
    ack.is_master = true;
    ack.allowed = true;

    zauto_lock l(_lock);

    uint64_t now = now_ms();
    auto node = beacon.from_addr;

    worker_map::iterator itr = _workers.find(node);
    if (itr == _workers.end())
    {
        // if is a new worker, check allow list first if need
        if (_use_allow_list && _allow_list.find(node) == _allow_list.end())
        {
            dwarn("new worker[%s] is rejected", node.to_string());
            ack.allowed = false;
            return;
        }

        // create new entry for node
        worker_record record(node, now);
        record.is_alive = true;
        _workers.insert(std::make_pair(node, record));

        report(node, false, true);
        on_worker_connected(node);
    }
    else if (is_time_greater_than(now, itr->second.last_beacon_recv_time))
    {
        // update last_beacon_recv_time
        itr->second.last_beacon_recv_time = now;

        if (itr->second.is_alive == false)
        {
            itr->second.is_alive = true;

            report(node, false, true);
            on_worker_connected(node);
        }
    }
}

void failure_detector::on_ping(const beacon_msg& beacon, ::dsn::rpc_replier<beacon_ack>& reply)
{
    beacon_ack ack;
    on_ping_internal(beacon, ack);
    reply(ack);
}

void failure_detector::end_ping(::dsn::error_code err, const beacon_ack& ack, void*)
{
    end_ping_internal(err, ack);
}

bool failure_detector::end_ping_internal(::dsn::error_code err, const beacon_ack& ack)
{
    /*
     * the caller of the end_ping_internal should lock necessarily!!!
     */
    uint64_t beacon_send_time = ack.time;
    auto node = ack.this_node;
    uint64_t now = now_ms();

    master_map::iterator itr = _masters.find(node);

    if (itr == _masters.end())
    {
        dwarn("received beacon ack without corresponding master, ignore it, "
            "remote_master[%s], local_worker[%s]",
            node.to_string(), primary_address().to_string());
        err.end_tracking();
        return false;
    }

    master_record& record = itr->second;
    if (!ack.allowed)
    {
        dwarn("worker rejected, stop sending beacon message, "
            "remote_master[%s], local_worker[%s]",
            node.to_string(), primary_address().to_string());
        record.rejected = true;
        record.send_beacon_timer->cancel(true);
        err.end_tracking();
        return false;
    }

    if (!is_time_greater_than(beacon_send_time, record.last_send_time_for_beacon_with_ack))
    {
        // out-dated beacon acks, do nothing
        dinfo("ignore out dated beacon acks");
        err.end_tracking();
        return false;
    }

    // now the ack is applicable

    if (err != ERR_OK)
    {
        dwarn("ping master failed, err=%s", err.to_string());
        return true;
    }

    // update last_send_time_for_beacon_with_ack
    record.last_send_time_for_beacon_with_ack = beacon_send_time;
    record.rejected = false;

    if (record.is_alive == false
        && now - record.last_send_time_for_beacon_with_ack <= _lease_milliseconds)
    {
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

    if (it != _masters.end())
    {
        it->second.send_beacon_timer->cancel(true);
        _masters.erase(it);
        dinfo("unregister master[%s] successfully", node.to_string());
        return true;
    }
    else
    {
        ddebug("unregister master[%s] failed, cannot find it in FD", node.to_string());
        return false;
    }
}

bool failure_detector::is_master_connected( ::dsn::rpc_address node) const
{
    zauto_lock l(_lock);
    auto it = _masters.find(node);
    if (it != _masters.end())
        return it->second.is_alive;
    else
        return false;
}

void failure_detector::register_worker( ::dsn::rpc_address target, bool is_connected)
{
    uint64_t now = now_ms();

    /*
     * callers should use the fd::_lock necessarily
     */
    worker_record record(target, now);
    record.is_alive = is_connected ? true : false;

    auto ret = _workers.insert(std::make_pair(target, record));
    if ( ret.second )
    {
        dinfo("register worker[%s] successfully", target.to_string());
    }
    else
    {
        dinfo("worker[%s] already registered", target.to_string());
    }
}

bool failure_detector::unregister_worker(::dsn::rpc_address node)
{
    /*
     * callers should use the fd::_lock necessarily
     */
    bool ret;

    size_t count = _workers.erase(node);

    if ( count == 0 )
    {
        ret = false;
    }
    else
    {
        ret = true;
    }

    dinfo("unregister worker[%s] successfully, removed entry count is %u",
          node.to_string(), (uint32_t)count);

    return ret;
}

void failure_detector::clear_workers()
{
    zauto_lock l(_lock);
    _workers.clear();
}

bool failure_detector::is_worker_connected( ::dsn::rpc_address node) const
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
    beacon.from_addr= primary_address();
    beacon.to_addr = target;

    dinfo("send ping message, from[%s], to[%s], time[%" PRId64 "]",
          beacon.from_addr.to_string(), beacon.to_addr.to_string(), time);

    ::dsn::rpc::call(
        target,
        RPC_FD_FAILURE_DETECTOR_PING,
        beacon,
        this,
        [=](error_code err, beacon_ack&& resp)
        {
            if (err != ::dsn::ERR_OK)
            {
                beacon_ack ack;
                ack.time = beacon.time;
                ack.this_node = beacon.to_addr;
                ack.primary_node.set_invalid();
                ack.is_master = false;
                ack.allowed = true;
                end_ping(err, ack, nullptr);
            }
            else
            {
                end_ping(err, std::move(resp), nullptr);
            }
        },
        std::chrono::milliseconds(_check_interval_milliseconds)
        );
}


}} // end namespace
