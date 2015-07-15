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

# include <dsn/dist/failure_detector.h>
# include <chrono>
# include <ctime>
# include <dsn/internal/serialization.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "failure_detector"

using namespace ::dsn::service;

namespace dsn { 
namespace fd {

failure_detector::failure_detector()
{
    auto pool = task_spec::get(LPC_BEACON_CHECK)->pool_code;
    task_spec::get(RPC_FD_FAILURE_DETECTOR_PING)->pool_code = pool;
    task_spec::get(RPC_FD_FAILURE_DETECTOR_PING_ACK)->pool_code = pool;
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
    _current_task = tasking::enqueue(LPC_BEACON_CHECK, this, &failure_detector::process_all_records, -1, _check_interval_milliseconds, _check_interval_milliseconds);

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

    if (_current_task != nullptr)
    {
        _current_task->cancel(true);
        _current_task = nullptr;
    }

    return ERR_OK;
}

void failure_detector::register_master(const end_point& target)
{
    uint64_t now = now_ms();

    zauto_lock l(_lock);

    master_record record(target, now, now + _beacon_interval_milliseconds);

    auto ret = _masters.insert(std::make_pair(target, record));
    if (ret.second)
    {
        dinfo(
            "register_rpc_handler master successfully, target machine ip [%u], port[%u]",
            target.ip, static_cast<int>(target.port));
    }
    else
    {
        // active the beacon again in case previously local node is not in target's allow list
        ret.first->second.rejected = false;
        dinfo(
            "master already registered, for target machine: target machine ip [%u], port[%u]",
            target.ip, static_cast<int>(target.port));
    }

    send_beacon(target, now_ms());
}

bool failure_detector::switch_master(const end_point& from, const end_point& to)
{
    {
        zauto_lock l(_lock);

        auto it = _masters.find(from);
        auto it2 = _masters.find(to);
        if (it != _masters.end())
        {
            if (it2 != _masters.end())
            {
                dinfo(
                    "master switch, switch master from %s:%d to %s:%d failed as both are already registered",
                    from.name.c_str(), static_cast<int>(from.port),
                    to.name.c_str(), static_cast<int>(to.port)
                    );
                return false;
            }

            it->second.node = to;
            it->second.rejected = false;
            _masters.insert(std::make_pair(to, it->second));
            _masters.erase(from);

            dinfo(
                "master switch, switch master from %s:%d to %s:%d succeeded",
                from.name.c_str(), static_cast<int>(from.port),
                to.name.c_str(), static_cast<int>(to.port)
                );
        }
        else
        {
            dinfo(
                "master switch, switch master from %s:%d to %s:%d failed as the former has not been registered yet",
                from.name.c_str(), static_cast<int>(from.port),
                to.name.c_str(), static_cast<int>(to.port)
                );
            return false;
        }
    }

    send_beacon(to, now_ms());
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

void failure_detector::report(const end_point& node, bool is_master, bool is_connected)
{
    ddebug("%s %s:%hu %sconnected", is_master ? "master":"worker", node.name.c_str(), node.port, is_connected ? "" : "dis");

    printf ("%s %s:%hu %sconnected\n", is_master ? "master":"worker", node.name.c_str(), node.port, is_connected ? "" : "dis");    
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

void failure_detector::process_all_records()
{
    if (!_is_started)
    {
        return;
    }


    zauto_lock l(_lock);

    std::vector<end_point> expire;
    uint64_t now =now_ms();

    master_map::iterator itr = _masters.begin();
    for (; itr != _masters.end() ; itr++)
    {
        master_record& record = itr->second;
        if (is_time_greater_than(now, record.next_beacon_time))
        {
            if (!record.rejected || random32(0, 40) <= 10)
            {
                record.next_beacon_time = now + _beacon_interval_milliseconds;
                send_beacon(record.node, now);
            }
        }

        if (record.is_alive 
            && now - record.last_send_time_for_beacon_with_ack >= _lease_milliseconds)
        {
            expire.push_back(record.node);
            record.is_alive = false;

            report(record.node, true, false);
        }
    }

    if ( expire.size() > 0 )
    {
        on_master_disconnected(expire);
    }

    // process recv record, for server
    expire.clear();
    now =now_ms();
    
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
    
    if ( expire.size() > 0 )
    {
        on_worker_disconnected(expire);
    }
}

void failure_detector::add_allow_list( const end_point& node)
{
    zauto_lock l(_lock);
    _allow_list.insert(node);
}

bool failure_detector::remove_from_allow_list( const end_point& node)
{
    zauto_lock l(_lock);
    return _allow_list.erase(node) > 0;
}

void failure_detector::on_ping_internal(const beacon_msg& beacon, __out_param beacon_ack& ack)
{
    ack.is_master = true;
    ack.this_node = beacon.to;
    ack.primary_node = primary_address();
    ack.time = beacon.time;
    ack.allowed = true;

    zauto_lock l(_lock);

    uint64_t now = now_ms();
    auto node = beacon.from;

    worker_map::iterator itr = _workers.find(node);
    if (itr == _workers.end())
    {
        if (_use_allow_list && _allow_list.find(node) == _allow_list.end())
        {
            ddebug("Client %s:%hu is rejected", node.name.c_str(), node.port);
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
        itr->second.last_beacon_recv_time = now;

        if (itr->second.is_alive == false)
        {
            itr->second.is_alive = true;

            report(node, false, true);
            on_worker_connected(node);
        }
    }
}

void failure_detector::on_ping(const beacon_msg& beacon, ::dsn::service::rpc_replier<beacon_ack>& reply)
{
    beacon_ack ack;
    on_ping_internal(beacon, ack);
    reply(ack);
}

void failure_detector::end_ping(::dsn::error_code err, const beacon_ack& ack, void* context)
{
    if (err != ERR_OK) return;

    uint64_t beacon_send_time = ack.time;
    auto node = ack.this_node;

    zauto_lock l(_lock);

    uint64_t now = now_ms();

    master_map::iterator itr = _masters.find(node);

    if ( itr == _masters.end() )
    {
        dwarn("Failure in process beacon ack in liveness monitor, received beacon ack without corresponding beacon record, remote node name[%s], local node name[%s]",
            node.name.c_str(), primary_address().name.c_str());

        return;
    }

    master_record& record = itr->second;
    if (!ack.allowed)
    {
        ddebug( "Server %s:%hu rejected me as i'm not in its allow list, stop sending beacon message", node.name.c_str(), node.port);
        record.rejected = true;
        return;
    }

    if (is_time_greater_than(beacon_send_time, record.last_send_time_for_beacon_with_ack))
    {
        record.last_send_time_for_beacon_with_ack = beacon_send_time;
        record.rejected = false;
    }
    else
    {
        return;
    }

    if (record.is_alive == false
        && now - record.last_send_time_for_beacon_with_ack <= _lease_milliseconds)
    {
        report(node, true, true);
        itr->second.is_alive = true;
        on_master_connected(node);
    }
}

bool failure_detector::unregister_master(const end_point & node)
{
    zauto_lock l(_lock);

    bool ret;

    size_t count = _masters.erase(node);

    if ( count == 0 )
    {
        ret = false;
    }
    else
    {
        ret = true;
    }

    dinfo("remove send record sucessfully, removed node [%s], removed entry count [%u]",
        node.name.c_str(), (uint32_t)count);
    
    return ret;
}

bool failure_detector::is_master_connected( const end_point& node) const
{
    zauto_lock l(_lock);
    auto it = _masters.find(node);
    if (it != _masters.end())
        return it->second.is_alive;
    else
        return false;
}

void failure_detector::register_worker( const end_point& target, bool is_connected)
{
    uint64_t now = now_ms();

    zauto_lock l(_lock);

    worker_record record(target, now);
    record.is_alive = is_connected ? true : false;

    auto ret = _workers.insert(std::make_pair(target, record));
    if ( ret.second )
    {
        dinfo(
            "register_rpc_handler worker successfully", "target machine ip [%u], port[%u]",
            target.ip, static_cast<int>(target.port));
    }
    else
    {
        dinfo(       
            "worker already registered", "for target machine: target machine ip [%u], port[%u]",
            target.ip, static_cast<int>(target.port));
    }
}

bool failure_detector::unregister_worker(const end_point& node)
{
    zauto_lock l(_lock);

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

    dinfo("remove recv record sucessfully, removed node [%s], removed entry count [%u]",
        node.name.c_str(), (uint32_t)count);
    return ret;
}

void failure_detector::clear_workers()
{
    zauto_lock l(_lock);
    _workers.clear();
}

bool failure_detector::is_worker_connected( const end_point& node) const
{
    zauto_lock l(_lock);
    auto it = _workers.find(node);
    if (it != _workers.end())
        return it->second.is_alive;
    else
        return false;
}

void failure_detector::send_beacon(const end_point& target, uint64_t time)
{
    beacon_msg beacon;
    beacon.time = time;
    beacon.from = primary_address();
    beacon.to = target;

    begin_ping(
        beacon,
        nullptr,
        0,
        static_cast<int>(_check_interval_milliseconds),
        0,
        &target
        );
}

}} // end namespace
