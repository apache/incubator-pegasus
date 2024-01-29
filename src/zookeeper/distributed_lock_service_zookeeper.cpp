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

#include <zookeeper/zookeeper.h>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "distributed_lock_service_zookeeper.h"
#include "lock_struct.h"
#include "lock_types.h"
#include "runtime/service_app.h"
#include "runtime/task/async_calls.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"
#include "zookeeper/zookeeper_session_mgr.h"
#include "zookeeper_error.h"
#include "zookeeper_session.h"

namespace dsn {
namespace dist {

DSN_DECLARE_int32(timeout_ms);

std::string distributed_lock_service_zookeeper::LOCK_NODE_PREFIX = "LOCKNODE";

distributed_lock_service_zookeeper::distributed_lock_service_zookeeper() : ref_counter()
{
    _first_call = true;
}

distributed_lock_service_zookeeper::~distributed_lock_service_zookeeper()
{
    if (_session) {
        std::vector<lock_struct_ptr> handle_vec;
        {
            utils::auto_write_lock l(_service_lock);
            for (auto &kv : _zookeeper_locks)
                handle_vec.push_back(kv.second);
            _zookeeper_locks.clear();
        }
        for (lock_struct_ptr &ptr : handle_vec)
            _session->detach(ptr.get());
        _session->detach(this);

        _session = nullptr;
    }
}

error_code distributed_lock_service_zookeeper::finalize()
{
    release_ref();
    return ERR_OK;
}

void distributed_lock_service_zookeeper::erase(const lock_key &key)
{
    utils::auto_write_lock l(_service_lock);
    _zookeeper_locks.erase(key);
}

error_code distributed_lock_service_zookeeper::initialize(const std::vector<std::string> &args)
{
    if (args.empty()) {
        LOG_ERROR("need parameters: <lock_root>");
        return ERR_INVALID_PARAMETERS;
    }
    const char *lock_root = args[0].c_str();

    _session =
        zookeeper_session_mgr::instance().get_session(service_app::current_service_app_info());
    _zoo_state = _session->attach(this,
                                  std::bind(&distributed_lock_service_zookeeper::on_zoo_session_evt,
                                            lock_srv_ptr(this),
                                            std::placeholders::_1));
    if (_zoo_state != ZOO_CONNECTED_STATE) {
        _waiting_attach.wait_for(FLAGS_timeout_ms);
        if (_zoo_state != ZOO_CONNECTED_STATE) {
            LOG_WARNING(
                "attach to zookeeper session timeout, distributed lock service initialized failed");
            return ERR_TIMEOUT;
        }
    }

    std::vector<std::string> slices;
    utils::split_args(lock_root, slices, '/');
    std::string current = "";
    for (auto &str : slices) {
        utils::notify_event e;
        int zerr;
        current = current + "/" + str;
        zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();
        op->_optype = zookeeper_session::ZOO_CREATE;
        op->_input._path = current;
        op->_callback_function = [&e, &zerr](zookeeper_session::zoo_opcontext *op) mutable {
            zerr = op->_output.error;
            e.notify();
        };

        _session->visit(op);
        e.wait();
        if (zerr != ZOK && zerr != ZNODEEXISTS) {
            LOG_ERROR("create zk node failed, path = {}, err = {}", current, zerror(zerr));
            return from_zerror(zerr);
        }
    }
    _lock_root = current.empty() ? "/" : current;

    LOG_INFO("init distributed_lock_service_zookeeper succeed, lock_root = {}", _lock_root);
    // Notice: this reference is released in the finalize
    add_ref();
    return ERR_OK;
}

std::pair<task_ptr, task_ptr>
distributed_lock_service_zookeeper::lock(const std::string &lock_id,
                                         const std::string &myself_id,
                                         task_code lock_cb_code,
                                         const lock_callback &lock_cb,
                                         task_code lease_expire_code,
                                         const lock_callback &lease_expire_callback,
                                         const lock_options &opt)
{
    lock_struct_ptr handle;
    {
        utils::auto_write_lock l(_service_lock);
        auto id_pair = std::make_pair(lock_id, myself_id);
        auto iter = _zookeeper_locks.find(id_pair);
        if (iter == _zookeeper_locks.end()) {
            if (!opt.create_if_not_exist) {
                task_ptr tsk = tasking::enqueue(
                    lock_cb_code, nullptr, std::bind(lock_cb, ERR_OBJECT_NOT_FOUND, "", -1));
                return std::make_pair(tsk, nullptr);
            } else {
                handle = new lock_struct(lock_srv_ptr(this));
                handle->initialize(lock_id, myself_id);
                _zookeeper_locks[id_pair] = handle;
            }
        } else
            handle = iter->second;
    }

    lock_future_ptr lock_tsk(new lock_future(lock_cb_code, lock_cb, 0));
    lock_future_ptr expire_tsk(new lock_future(lease_expire_code, lease_expire_callback, 0));

    tasking::enqueue(TASK_CODE_DLOCK,
                     nullptr,
                     std::bind(&lock_struct::try_lock, handle, lock_tsk, expire_tsk),
                     handle->hash());
    return std::make_pair(lock_tsk, expire_tsk);
}

task_ptr distributed_lock_service_zookeeper::unlock(const std::string &lock_id,
                                                    const std::string &myself_id,
                                                    bool destroy,
                                                    task_code cb_code,
                                                    const err_callback &cb)
{
    lock_struct_ptr handle;
    {
        utils::auto_read_lock l(_service_lock);
        auto iter = _zookeeper_locks.find(std::make_pair(lock_id, myself_id));
        if (iter == _zookeeper_locks.end())
            return tasking::enqueue(cb_code, nullptr, std::bind(cb, ERR_OBJECT_NOT_FOUND));
        handle = iter->second;
    }
    error_code_future_ptr unlock_tsk(new error_code_future(cb_code, cb, 0));
    tasking::enqueue(TASK_CODE_DLOCK,
                     nullptr,
                     std::bind(&lock_struct::unlock, handle, unlock_tsk),
                     handle->hash());
    return unlock_tsk;
}

task_ptr distributed_lock_service_zookeeper::cancel_pending_lock(const std::string &lock_id,
                                                                 const std::string &myself_id,
                                                                 task_code cb_code,
                                                                 const lock_callback &cb)
{
    lock_struct_ptr handle;
    {
        utils::auto_read_lock l(_service_lock);
        auto iter = _zookeeper_locks.find(std::make_pair(lock_id, myself_id));
        if (iter == _zookeeper_locks.end())
            return tasking::enqueue(cb_code, nullptr, std::bind(cb, ERR_OBJECT_NOT_FOUND, "", -1));
        handle = iter->second;
    }
    lock_future_ptr cancel_tsk(new lock_future(cb_code, cb, 0));
    tasking::enqueue(TASK_CODE_DLOCK,
                     nullptr,
                     std::bind(&lock_struct::cancel_pending_lock, handle, cancel_tsk),
                     handle->hash());
    return cancel_tsk;
}

task_ptr distributed_lock_service_zookeeper::query_lock(const std::string &lock_id,
                                                        task_code cb_code,
                                                        const lock_callback &cb)
{
    std::string owner = "";
    uint64_t version = -1;
    error_code ec = query_cache(lock_id, owner, version);
    return tasking::enqueue(cb_code, nullptr, std::bind(cb, ec, owner, version));
}

error_code distributed_lock_service_zookeeper::query_cache(const std::string &lock_id,
                                                           /*out*/ std::string &owner,
                                                           /*out*/ uint64_t &version)
{
    utils::auto_read_lock l(_service_lock);
    auto iter = _lock_cache.find(lock_id);
    if (_lock_cache.end() == iter)
        return ERR_OBJECT_NOT_FOUND;
    else {
        owner = iter->second.first;
        version = iter->second.second;
        return ERR_OK;
    }
}

void distributed_lock_service_zookeeper::refresh_lock_cache(const std::string &lock_id,
                                                            const std::string &owner,
                                                            uint64_t version)
{
    utils::auto_write_lock l(_service_lock);
    _lock_cache[lock_id] = std::make_pair(owner, version);
}

void distributed_lock_service_zookeeper::dispatch_zookeeper_session_expire()
{
    utils::auto_read_lock l(_service_lock);
    for (auto &kv : _zookeeper_locks)
        tasking::enqueue(TASK_CODE_DLOCK,
                         nullptr,
                         std::bind(&lock_struct::lock_expired, kv.second),
                         kv.second->hash());
}

/*static*/
/* this function runs in zookeeper do-completion thread */
void distributed_lock_service_zookeeper::on_zoo_session_evt(lock_srv_ptr _this, int zoo_state)
{
    // TODO: better policy of zookeeper session response
    _this->_zoo_state = zoo_state;

    if (_this->_first_call && ZOO_CONNECTED_STATE == zoo_state) {
        _this->_first_call = false;
        _this->_waiting_attach.notify();
        return;
    }

    if (ZOO_EXPIRED_SESSION_STATE == zoo_state || ZOO_AUTH_FAILED_STATE == zoo_state) {
        LOG_ERROR("get zoo state: {}, which means the session is expired",
                  zookeeper_session::string_zoo_state(zoo_state));
        _this->dispatch_zookeeper_session_expire();
    } else {
        LOG_WARNING("get zoo state: {}, ignore it", zookeeper_session::string_zoo_state(zoo_state));
    }
}
}
}
