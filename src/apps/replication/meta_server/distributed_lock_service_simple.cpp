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
        DEFINE_TASK_CODE(LPC_DIST_LOCK_SVC_CALLBACK, TASK_PRIORITY_COMMON, THREAD_POOL_META_SERVER);

        void distributed_lock_service_simple::create_lock(const std::string& lock_id,
            const err_callback& cb)
        {
            dassert(!"not used", "");
        }

        void distributed_lock_service_simple::destroy_lock(const std::string& lock_id,
            const err_callback& cb)
        {
            dassert(!"not used", "");
        }

        void distributed_lock_service_simple::lock(const std::string& lock_id,
            const std::string& myself_id,
            bool create_if_not_exist,
            const err_string_callback& cb)
        {
            lock_info li;
            error_code err;
            std::string cowner;

            {
                zauto_lock l(_lock);
                auto it = _dlocks.find(lock_id);
                if (it == _dlocks.end())
                {
                    if (!create_if_not_exist)
                        err = ERR_OBJECT_NOT_FOUND;
                    else
                    {
                        li.owner = myself_id;
                        li.cb = cb;
                        _dlocks.insert(locks::value_type(lock_id, li));
                        err = ERR_OK;
                        cowner = myself_id;
                    }
                }
                else
                {
                    if (it->second.owner != "")
                    {
                        if (it->second.owner == myself_id)
                            err = ERR_RECURSIVE_LOCK;
                        else
                            err = ERR_HOLD_BY_OTHERS;
                        cowner = it->second.owner;
                    }
                    else
                    {
                        it->second.owner = myself_id;
                        it->second.cb = cb;
                        err = ERR_OK;
                        cowner = myself_id;
                    }
                }
            }

            tasking::enqueue(
                LPC_DIST_LOCK_SVC_CALLBACK,
                nullptr,
                [=]() { cb(err, cowner); }
            );
        }

        void distributed_lock_service_simple::unlock(const std::string& lock_id,
            const std::string& myself_id,
            bool destroy,
            const err_callback& cb)
        {
            error_code err;

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
                        it->second.owner = "";
                        it->second.cb = nullptr;
                        err = ERR_OK;
                    }
                }
            }

            tasking::enqueue(
                LPC_DIST_LOCK_SVC_CALLBACK,
                nullptr,
                [=]() { cb(err); }
            );
        }

        void distributed_lock_service_simple::query_lock(const std::string& lock_id,
            const err_string_callback& cb)
        {
            error_code err;
            std::string cowner;

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
                }
            }

            tasking::enqueue(
                LPC_DIST_LOCK_SVC_CALLBACK,
                nullptr,
                [=]() { cb(err, cowner); }
            );
        }
    }
}

