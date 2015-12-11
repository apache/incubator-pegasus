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
 *     distributed lock service implemented with zookeeper
 *
 * Revision history:
 *     2015-12-04, @shengofsun (sunweijie@xiaomi.com)
 */
#include <dsn/dist/replication/replication.codes.h>
#include <zookeeper.h>
#include <boost/lexical_cast.hpp>
#include <functional>
#include <algorithm>
#include <utility>

#include "zookeeper_session.h"
#include "distributed_lock_service_zookeeper.h"
#include "lock_struct.h"
#include "lock_types.h"

#include "zookeeper_error.h"

#ifdef __TITLE__
#undef __TITLE__
#endif

#define __TITLE__ "dlock.service.zk"

namespace dsn { namespace dist {

std::string distributed_lock_service_zookeeper::LOCK_NODE_PREFIX = "LOCKNODE";

distributed_lock_service_zookeeper::distributed_lock_service_zookeeper(): clientlet(), ref_counter()
{
    _first_call = true;
}

distributed_lock_service_zookeeper::~distributed_lock_service_zookeeper()
{
    if (_session)
    {
        std::vector<lock_struct_ptr> handle_vec;
        {
            utils::auto_write_lock l(_service_lock);
            for (auto& kv: _zookeeper_locks)
                handle_vec.push_back(kv.second);
            _zookeeper_locks.clear();
        }
        for (lock_struct_ptr& ptr: handle_vec)
            _session->detach(ptr.get());
        _session->detach(this);

        _session = nullptr;
    }
}

void distributed_lock_service_zookeeper::erase(const lock_key& key)
{
    utils::auto_write_lock l(_service_lock);
    _zookeeper_locks.erase(key);
}

error_code distributed_lock_service_zookeeper::initialize(const char* /*work_dir*/, const char* lock_root)
{
    dsn_app_info node;
    if (!dsn_get_current_app_info(&node))
    {
        derror("get current app info failed, can not init zookeeper lock");
        return ERR_CORRUPTION;
    }

    _session = zookeeper_session_mgr::instance().get_session(&node);
    _zoo_state = _session->attach(this, std::bind(&distributed_lock_service_zookeeper::on_zoo_session_evt,
                                                  lock_srv_ptr(this),
                                                  std::placeholders::_1) );
    if (_zoo_state != ZOO_CONNECTED_STATE)
    {
        _waiting_attach.wait_for( zookeeper_session_mgr::fast_instance().timeout() );
        if (_zoo_state != ZOO_CONNECTED_STATE) {
            dwarn("attach to zookeeper session timeout, distributed lock service initialized failed");
            return ERR_TIMEOUT;
        }
    }

    std::vector<std::string> slices;
    utils::split_args(lock_root, slices, '/');
    std::string current = "";
    for (auto& str: slices)
    {
        if (str.empty())
            continue;
        utils::notify_event e;
        int zerr;
        current = current + "/" + str;
        zookeeper_session::zoo_opcontext* op = zookeeper_session::create_context();
        op->_optype = zookeeper_session::ZOO_CREATE;
        op->_input._path = current;
        op->_callback_function = [&e, &zerr](zookeeper_session::zoo_opcontext* op) mutable
        {
            zerr = op->_output.error;
            e.notify();
        };

        _session->visit(op);
        e.wait();
        if (zerr != ZOK && zerr != ZNODEEXISTS)
        {
            derror("create zk node failed, path = %s, err = %s", current.c_str(), zerror(zerr));
            return from_zerror(zerr);
        }
    }
    _lock_root = current.empty() ? "/" : current;
    ddebug("init distributed_lock_service_zookeeper succeed, lock_root = %s", _lock_root.c_str());

    // TODO: add_ref() here because we need add_ref/release_ref in callbacks, so this object should be
    // stored in ref_ptr to avoid memory leak.
    add_ref();
    return ERR_OK;
}

std::pair<task_ptr, task_ptr> distributed_lock_service_zookeeper::lock(
    const std::string &lock_id,
    const std::string &myself_id,
    bool create_if_not_exist,
    task_code lock_cb_code,
    const lock_callback &lock_cb,
    task_code lease_expire_code,
    const lock_callback &lease_expire_callback)
{
    lock_struct_ptr handle;
    {
        utils::auto_write_lock l(_service_lock);
        auto id_pair = std::make_pair(lock_id, myself_id);
        auto iter = _zookeeper_locks.find( id_pair );
        if ( iter==_zookeeper_locks.end() ){
            if (!create_if_not_exist) {
                task_ptr tsk = tasking::enqueue(lock_cb_code, nullptr, 
                                                std::bind(lock_cb, ERR_OBJECT_NOT_FOUND, "", -1));
                return std::make_pair(tsk, nullptr);
            }
            else {
                handle = new lock_struct(lock_srv_ptr(this));
                handle->initialize(lock_id, myself_id);
                _zookeeper_locks[ id_pair ] = handle;
            }
        }
        else
            handle = iter->second;
    }

    auto lock_tsk = tasking::create_late_task<distributed_lock_service::lock_callback>(
        lock_cb_code, 
        lock_cb
    );
    auto expire_tsk = tasking::create_late_task<distributed_lock_service::lock_callback>(
        lease_expire_code,
        lease_expire_callback
    );
    
    task_ptr ref_holder1(lock_tsk), ref_holder2(expire_tsk);
    tasking::enqueue(TASK_CODE_DLOCK, nullptr, std::bind(&lock_struct::try_lock, handle, lock_tsk, expire_tsk), handle->hash());
    return std::make_pair(ref_holder1, ref_holder2);
}

task_ptr distributed_lock_service_zookeeper::unlock(
    const std::string& lock_id, 
    const std::string& myself_id, 
    bool destroy, 
    task_code cb_code, 
    const err_callback& cb)
{
    lock_struct_ptr handle;
    {
        utils::auto_read_lock l(_service_lock);
        auto iter = _zookeeper_locks.find( std::make_pair(lock_id, myself_id) );
        if (iter == _zookeeper_locks.end())
            return tasking::enqueue(cb_code, nullptr, std::bind(cb, ERR_OBJECT_NOT_FOUND));
        handle = iter->second;
    }
    auto unlock_tsk = tasking::create_late_task<distributed_lock_service::err_callback>(cb_code, cb);
    task_ptr ref_holder(unlock_tsk);
    tasking::enqueue(TASK_CODE_DLOCK, nullptr, std::bind(&lock_struct::unlock, handle, unlock_tsk), handle->hash());
    return ref_holder;
}

task_ptr distributed_lock_service_zookeeper::cancel_pending_lock(
    const std::string& lock_id,
    const std::string& myself_id,
    task_code cb_code,
    const lock_callback& cb)    
{
    lock_struct_ptr handle;
    {
        utils::auto_read_lock l(_service_lock);
        auto iter = _zookeeper_locks.find( std::make_pair(lock_id, myself_id) );
        if (iter == _zookeeper_locks.end())
            return tasking::enqueue(cb_code, nullptr, std::bind(cb, ERR_OBJECT_NOT_FOUND, "", -1));
        handle = iter->second;        
    }
    auto cancel_tsk = tasking::create_late_task<distributed_lock_service::lock_callback>(cb_code, cb);
    task_ptr ref_holder(cancel_tsk);
    tasking::enqueue(TASK_CODE_DLOCK, nullptr, std::bind(&lock_struct::cancel_pending_lock, handle, cancel_tsk), handle->hash());
    return ref_holder;
}

task_ptr distributed_lock_service_zookeeper::query_lock(
    const std::string& lock_id, 
    task_code cb_code, 
    const lock_callback& cb)
{
    lock_struct_ptr handle = nullptr;
    {
        utils::auto_read_lock l(_service_lock);
        for (auto& lock_pair: _zookeeper_locks)
        {
            if (lock_pair.first.first == lock_id)
            {
                handle = lock_pair.second;
                break;
            }
        }
    }
    if (handle == nullptr)
        return tasking::enqueue(cb_code, nullptr, std::bind(cb, ERR_OBJECT_NOT_FOUND, "", -1)); 
    else {
        auto query_tsk = tasking::create_late_task<distributed_lock_service::lock_callback>(cb_code, cb);
        task_ptr ref_holder(query_tsk);
        tasking::enqueue(TASK_CODE_DLOCK, nullptr, std::bind(&lock_struct::query, handle, query_tsk), handle->hash() );
        return ref_holder;
    }
}

void distributed_lock_service_zookeeper::dispatch_zookeeper_session_expire()
{
    utils::auto_read_lock l(_service_lock);
    for (auto& kv: _zookeeper_locks)
        tasking::enqueue(TASK_CODE_DLOCK, nullptr, std::bind(&lock_struct::lock_expired, kv.second), kv.second->hash());
}

/*static*/
/* this function runs in zookeeper do-completion thread */
void distributed_lock_service_zookeeper::on_zoo_session_evt(lock_srv_ptr _this, int zoo_state)
{
    //TODO: better policy of zookeeper session response
    _this->_zoo_state = zoo_state;

    if (_this->_first_call && ZOO_CONNECTED_STATE==zoo_state)
    {
        _this->_first_call = false;
        _this->_waiting_attach.notify();
    }
    else if (ZOO_CONNECTED_STATE!=zoo_state)
    {
        _this->dispatch_zookeeper_session_expire();
    }
}

}}
