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
*     interface of the reliable distributed lock service
*
* Revision history:
*     2015-10-28, Weijie Sun, first version
*     2015-11-5, @imzhenyu (Zhenyu Guo), remove create and destroy API as they are
*                unnecessary, adjust the interface, so that
*                (1) return task_ptr for callers to cancel or wait;
*                (2) add factory for provider registration; 
*                (3) add cb_code parameter, then users can specify where the callback
*                    should be executed
*     xxxx-xx-xx, author, fix bug about xxx
*/

# pragma once

# include <dsn/service_api_cpp.h>
# include <dsn/dist/error_code.h>
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
            template <typename T> static distributed_lock_service* create()
            {
                return new T();
            }

            typedef distributed_lock_service* (*factory)();

        public:
            typedef std::function<void (error_code ec)> err_callback;
            typedef std::function<void (error_code ec, const std::string& owner_id, uint64_t version)> lock_callback;

            struct lock_options {
                bool create_if_not_exist;
                bool create_enable_cache;
            };

            virtual ~distributed_lock_service() {}
            /*
             * initialization routine
             */
            virtual error_code initialize(const std::vector<std::string>& args) = 0;
            
            /*
             * finalize routine
             */
            virtual error_code finalize() = 0;

            /*
             * lock
             * lock_cb_code: the task code specifies where to execute the callback
             * lock_cb: the callback is executed when there are error or lock granted
             * lease_expire_code: the task code specifies where to execute the callback
             * lease_expire_callback: the callback is executed when lease is expired 
             *                        and unlock is not invoked
             * create_if_not_exist:
             *   if distributed lock for lock_id doesn't exist, try to create one
             * 
             * return:
             *   the first task handle for lock granted or error
             *   the second task is valid only when lock granted, and it is for lease expire
             *
             * possible ec:
             *   ERR_INVALID_PARAMETERS, lock_id invalid, or cb==nullptr
             *   ERR_TIMEOUT, creating lock timeout if create_if_not_exist==true
             *   ERR_OBJECT_NOT_FOUND, lock doesn't exist and create_if_not_exist == false
             *
             *   ERR_OK, the caller gets the lock. when the lock is hold by others, the callback
             *   is hold until it gets the lock.
             *   ERR_RECURSIVE_LOCK, call "lock" again if it was called before in the process
             *     context with the same parameter pair.
             */
            virtual std::pair<task_ptr, task_ptr> lock(
                              const std::string& lock_id,
                              const std::string& myself_id,
                              task_code lock_cb_code,
                              const lock_callback& lock_cb,
                              task_code lease_expire_code,
                              const lock_callback& lease_expire_callback, 
                              const lock_options& opt
                              ) = 0;
            
            /*
            * cancel the lock operation that is on pending
            * cb_code: the task code specifies where to execute the callback
            * lock_id should be valid, and cb should not be empty
            *
            * possible ec:
            *   ERR_INVALID_PARAMETERS
            *   ERR_OK, the pending lock is cancelled successfully
            *   ERR_OBJECT_NOT_FOUND, the caller is not found in pending list, check
            *   returned owner to see whether it already succeedes
            *
            */
            virtual task_ptr cancel_pending_lock(
                const std::string& lock_id,
                const std::string& myself_id,
                task_code cb_code,
                const lock_callback& cb) = 0;

            /*
             * unlock
             * cb_code: the task code specifies where to execute the callback
             * lock_id should be valid, and cb should not be empty
             *
             * possible ec:
             *   ERR_INVALID_PARAMETERS
             *   ERR_OK, release the lock successfully; if destroy==true, it also implies
             *     that the lock is destroy successfully.
             *   ERR_HOLD_BY_OTHERS, the lock is hold by others
             *   ERR_TIMEOUT, operation timeout. If destroy==false, this implies the unlock-op
             *     is timout; if destroy==true, it may be unlock-op or destroy-op who times out.
             *     For the latter, user can use query_lock to check the status of the lock
             */
            virtual task_ptr unlock(
                                const std::string& lock_id,
                                const std::string& myself_id,
                                bool destroy,
                                task_code cb_code,
                                const err_callback& cb) = 0;

            /*
             * cb_code: the task code specifies where to execute the callback
             * cb shouldn't be empty
             * possible ec:
             *   ERR_OK: the lock is hold by someone, user can get the owner by
             *     owner_id
             *   ERR_OBJECT_NOT_FOUND: the lock doesn't exist
             *   ERR_NO_OWNER, no one owns the lock
             *   ERR_TIMEOUT, operation timeout
             */
            virtual task_ptr query_lock(
                                    const std::string& lock_id,
                                    task_code cb_code,
                                    const lock_callback& cb) = 0;
            /*
             * error_code: err_invalid_parameters -> if the lock is created without cache enabled
             *             err_object_not_found -> no lock created with lock_id
             *             err_ok -> query the cache successfully
             */
            virtual error_code query_cache(
                const std::string& lock_id, 
                /*out*/std::string& owner, 
                /*out*/uint64_t& version) = 0;
        };
    }
}
