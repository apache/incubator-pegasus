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
 *     a simple version of distributed lock service for development
 *
 * Revision history:
 *     2015-11-04, @imzhenyu (Zhenyu.Guo@microsoft.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "distributed_lock_service_simple.h"
# include "replication_common.h"

namespace dsn
{
    namespace dist
    {
        DEFINE_TASK_CODE(LPC_DIST_LOCK_SVC_RANDOM_EXPIRE, TASK_PRIORITY_COMMON, THREAD_POOL_META_SERVER);

        static void __lock_cb_bind_and_enqueue(
            task_ptr lock_task,
            error_code err,
            const std::string& owner,
            uint64_t version,
            int delay_milliseconds = 0
            )
        {
            auto t = dynamic_cast<safe_late_task<distributed_lock_service::lock_callback>*>(lock_task.get());

            t->bind_and_enqueue(
                [&](distributed_lock_service::lock_callback& cb)
                {
                    return std::bind(cb, err, owner, version);
                },
                delay_milliseconds
                );
        }

        void distributed_lock_service_simple::random_lock_lease_expire(const std::string& lock_id)
        {
            // TODO: let's test without failure first
            return;

            std::string owner;
            uint64_t version;
            lock_wait_info next;
            task_ptr lease_callback;

            {
                zauto_lock l(_lock);
                auto it = _dlocks.find(lock_id);
                if (it != _dlocks.end())
                {
                    if (it->second.owner != "")
                    {
                        owner = it->second.owner;
                        version = it->second.version;
                        lease_callback = it->second.lease_callback;

                        if (it->second.pending_list.size() > 0)
                        {
                            next = it->second.pending_list.front();
                            
                            it->second.owner = next.owner;
                            it->second.version++;
                            it->second.lease_callback = next.lease_callback;
                            it->second.pending_list.pop_front();
                        }
                        else
                        {
                            next.owner = "";
                            it->second.owner = "";
                            it->second.version++;
                            it->second.lease_callback = nullptr;
                        }
                    }
                    else
                        return;
                }
                else
                {
                    dsn_task_cancel_current_timer();
                    return;
                }
            }
            
            __lock_cb_bind_and_enqueue(
                lease_callback,
                ERR_EXPIRED,
                owner,
                version,
                0
                );

            if (next.owner != "")
            {
                version++;
                error_code err = ERR_OK;
                __lock_cb_bind_and_enqueue(
                    next.grant_callback,
                    err,
                    next.owner,
                    version,
                    0
                    );
            }
        }

        error_code distributed_lock_service_simple::initialize(const char* /*work_dir*/, const char* /*lock_root*/)
        {
            return ERR_OK;
        }

        std::pair<task_ptr, task_ptr> distributed_lock_service_simple::lock(
            const std::string& lock_id,
            const std::string& myself_id,
            bool create_if_not_exist,
            task_code lock_cb_code,
            const lock_callback& lock_cb,
            task_code lease_expire_code,
            const lock_callback& lease_expire_callback
            )
        {
            task_ptr grant_cb = tasking::create_late_task(
                lock_cb_code,
                lock_cb,
                0,
                nullptr
                );

            task_ptr lease_cb = tasking::create_late_task(
                lease_expire_code,
                lease_expire_callback,
                0,
                nullptr
                );

            error_code err;
            std::string cowner;
            uint64_t version;
            bool is_new = false;

            {
                zauto_lock l(_lock);
                auto it = _dlocks.find(lock_id);
                if (it == _dlocks.end())
                {
                    if (!create_if_not_exist)
                        err = ERR_OBJECT_NOT_FOUND;
                    else
                    {
                        lock_info li;
                        li.owner = myself_id;
                        li.version = 1;
                        li.lease_callback = lease_cb;
                        _dlocks.insert(locks::value_type(lock_id, li));

                        err = ERR_OK;
                        cowner = myself_id;
                        version = 1;
                        is_new = true;
                    }
                }
                else
                {
                    if (it->second.owner != "")
                    {
                        if (it->second.owner == myself_id)
                        {
                            err = ERR_RECURSIVE_LOCK;
                            cowner = myself_id;
                            version = it->second.version;
                        }   
                        else
                        {
                            err = ERR_IO_PENDING;

                            lock_wait_info wi;
                            wi.grant_callback = grant_cb;
                            wi.lease_callback = lease_cb;
                            wi.owner = myself_id;
                            it->second.pending_list.push_back(wi);
                        }                        
                    }
                    else
                    {
                        it->second.lease_callback = lease_cb;
                        it->second.owner = myself_id;
                        it->second.version++;

                        err = ERR_OK;
                        cowner = myself_id;
                        version = it->second.version;
                    }
                }
            }

            if (is_new)
            {
                tasking::enqueue(
                    LPC_DIST_LOCK_SVC_RANDOM_EXPIRE,
                    this,
                    [=](){ random_lock_lease_expire(lock_id); },
                    0,
                    1000,
                    1000 * 60 * 5 // every 5 min
                    );
            }

            if (err != ERR_IO_PENDING)
            {
                __lock_cb_bind_and_enqueue(
                    grant_cb,
                    err,
                    cowner,
                    version
                    );
            }

            return std::pair<task_ptr, task_ptr>(grant_cb, lease_cb);
        }

        task_ptr distributed_lock_service_simple::cancel_pending_lock(
            const std::string& lock_id,
            const std::string& myself_id,
            task_code cb_code,
            const lock_callback& cb)
        {
            error_code err;
            std::string cowner;
            uint64_t version;

            {
                zauto_lock l(_lock);
                auto it = _dlocks.find(lock_id);
                if (it == _dlocks.end())
                {
                    err = ERR_OBJECT_NOT_FOUND;
                    cowner = "";
                    version = 0;
                }
                else
                {
                    cowner = it->second.owner;
                    version = it->second.version;
                    err = ERR_OBJECT_NOT_FOUND;
                    for (auto it2 = it->second.pending_list.begin();
                        it2 != it->second.pending_list.end();
                        it2++)
                    {
                        auto& w = *it2;
                        if (w.owner == myself_id)
                        {
                            err = ERR_OK;
                            it->second.pending_list.erase(it2);
                            break;
                        }
                    }
                }
            }

            return tasking::enqueue(
                cb_code,
                nullptr,
                [=]() { cb(err, cowner, version); }
            );
        }

        task_ptr distributed_lock_service_simple::unlock(
            const std::string& lock_id,
            const std::string& myself_id,
            bool destroy,
            task_code cb_code,
            const err_callback& cb)
        {
            error_code err;
            lock_wait_info next;
            uint64_t next_version;

            {
                zauto_lock l(_lock);
                auto it = _dlocks.find(lock_id);
                if (it == _dlocks.end())
                {
                    err = ERR_OBJECT_NOT_FOUND;
                }
                else
                {
                    if (it->second.owner != myself_id)
                    {
                        err = ERR_HOLD_BY_OTHERS;
                    }
                    else
                    {
                        err = ERR_OK;

                        if (it->second.pending_list.size() > 0)
                        {
                            next = it->second.pending_list.front();
                            next_version = it->second.version++;
                            it->second.owner = next.owner;
                            it->second.lease_callback = next.lease_callback;
                            it->second.pending_list.pop_front();
                        }
                        else
                        {
                            next.owner = "";
                            it->second.owner = "";
                            it->second.lease_callback = nullptr;
                            it->second.version++;
                        }
                    }
                }
            }

            auto t = tasking::enqueue(
                cb_code,
                nullptr,
                [=]() { cb(err); }
            );

            if (next.owner != "")
            {
                error_code err = ERR_OK;
                __lock_cb_bind_and_enqueue(
                    next.grant_callback,
                    err,
                    next.owner,
                    next_version,
                    0
                    );
            }

            return t;
        }

        task_ptr distributed_lock_service_simple::query_lock(
            const std::string& lock_id,
            task_code cb_code,
            const lock_callback& cb)
        {
            error_code err;
            std::string cowner;
            uint64_t version;

            {
                zauto_lock l(_lock);
                auto it = _dlocks.find(lock_id);
                if (it == _dlocks.end())
                {
                    err = ERR_OBJECT_NOT_FOUND;
                }
                else
                {
                    err = ERR_OK;
                    cowner = it->second.owner;
                    version = it->second.version;
                }
            }

            return tasking::enqueue(
                cb_code,
                nullptr,
                [=]() { cb(err, cowner, version); }
            );
        }
    }
}

