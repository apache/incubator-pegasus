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
# pragma once

# include <dsn/service_api_cpp.h>
# include <string>
# include <functional>
# include <utility>

namespace dsn
{
    namespace dist
    {
        class distributed_lock_service
        {
        public:
            typedef std::function<void (error_code ec)> err_callback;
            typedef std::function<void (error_code ec, const std::string& owner_id)> err_string_callback;
            /*
             * create a distributed lock
             * lock_id: the distributed lock_id shared by all
             * cb: control the return value, possible ec are:
             *   ERR_OK, create lock ok
             *   ERR_LOCK_ALREADY_EXIST, lock is created already
             *   ERR_TIMEOUT, operation timeout
             *   ERR_INVALID_PARAMETERS
             */
            virtual void create_lock(const std::string& lock_id,
                                     const err_callback& cb) = 0;
            /*
             * destroy a distrubted lock
             * possible ec:
             *   ERR_OK, destroy the lock ok
             *   ERR_OBJECT_NOT_FOUND, lock doesn't exist
             *   ERR_TIMEOUT, operation timeout
             *   ERR_HOLD_BY_OHTERS, someone else is holding or pending the lock
             *   ERR_INVALID_PARAMETERS
             */
            virtual void destroy_lock(const std::string& lock_id,
                                      const err_callback& cb) = 0;
            /*
             * lock
             * create_if_not_exist:
             *   if distributed lock for lock_id doesn't exist, try to create one
             *
             * possible ec:
             *   ERR_INVALID_PARAMETERS, lock_id invalid, or cb==nullptr
             *   ERR_TIMEOUT, creating lock timeout if create_if_not_exist==true
             *   ERR_OK, the caller gets the lock, owner_id is ignored
             *   ERR_HOLD_BY_OTHERS, another caller gets the lock, user can know
             *     the lock owner by owner_id
             *   ERR_RECURSIVE_LOCK, call "lock" again if it was called before in the process
             *     context with the same parameter pair.
             *   ERR_EXPIRED, the leasing period is expired. In this case, the lock is
             *     released implicitly, and the cb will never be called again.
             *
             * Note: cb may be called more than once. But there are some constraints
             *   (1) when user calls unlock for the same myself_id and the lock is released
             *       successfully, cb will never be called again
             *   (2) cb will never be called again after it returns ERR_EXPIRED
             *   (3) if cb returns ERR_OK, then ERR_HOLD_BY_OTHERS should never be returned
             */
            virtual void lock(const std::string& lock_id,
                              const std::string& myself_id,
                              bool create_if_not_exist,
                              const err_string_callback& cb) = 0;
            /*
             * unlock
             * lock_id should be valid, and cb should not be empty
             *
             * possible ec:
             *   ERR_INVALID_PARAMETERS
             *   ERR_OK, release the lock successfully; if destroy==true, it also implies
             *     that the lock is destroy successfully.
             *   ERR_HOLD_BY_OTHERS, the lock is released successfully, but destroy-op is
             *     failed as someone else is holding or pending the lock
             *   ERR_TIMEOUT, operation timeout. If destroy==false, this implies the unlock-op
             *     is timout; if destroy==true, it may be unlock-op or destroy-op who times out.
             *     For the latter, user can use query_lock to check the status of the lock
             */
            virtual void unlock(const std::string& lock_id,
                                const std::string& myself_id,
                                bool destroy,
                                const err_callback& cb) = 0;
            /*
             * cb shouldn't be empty
             * possible ec:
             *   ERR_OK: the lock is hold by someone, user can get the owner by
             *     owner_id
             *   ERR_OBJECT_NOT_FOUND: the lock doesn't exist
             *   ERR_NO_OWNER, no one owns the lock
             *   ERR_TIMEOUT, operation timeout
             */
            virtual void query_lock(const std::string& lock_id,
                                    const err_string_callback& cb) = 0;
        };
    }
}
