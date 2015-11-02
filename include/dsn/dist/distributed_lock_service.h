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
            class lock_context
            {
            protected:
                std::string _myself;
                std::string _owner;
            public:
                lock_context(const std::string &m): _myself(m), _owner() {}
                lock_context(std::string&& m): _myself(std::move(m)) {}
                virtual ~lock_context();
                const std::string& myself() const { return _myself; }
                const std::string& owner() const { return _owner; }
            };
            typedef lock_context* p_lockctx;
        public:
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
                                     const std::function<void (dsn_error_t ec)>& cb);
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
                                      const std::function<void (dsn_error_t ec)>& cb);
            /*
             * lock
             * the lock_id should be valid, the ctx shouldn't be nullptr, cb shouldn't be empty
             * possible ec:
             *   ERR_INVALID_PARAMETERS, lock_id invalid or ctx is nullptr or cb is empty
             *   ERR_OK, the caller gets the lock, then ctx is ignored.
             *   ERR_HOLD_BY_OTHERS, another caller gets the lock.
             *     Then the cb_ctx==ctx, and user can get the owner of the lock by
             *     cb_ctx->owner() or ctx->owner()
             *   ERR_EXPIRED, the leasing period is expired. In this case, the lock is
             *     released implicitly, and the cb will never be called again.
             *
             * Note: cb may be called more than once. But there are some constraints
             *   (1) when user calls unlock for the same lock_context and realeses the lock
             *       successfully, then cb will never be called again
             *   (2) cb returns ERR_EXPIRED
             *   (3) if cb returns ERR_OK, then ERR_OTHERS_HOLD_LOCK should never be returned
             */
            virtual void lock(const std::string& lock_id,
                              p_lockctx ctx,
                              const std::function<void (dsn_error_t ec, p_lockctx cb_ctx)>& cb);
            /*
             * unlock
             * lock_id should be valid, ctx shouldn't be nullptr, cb should not be empty
             *
             * possible ec:
             *   ERR_INVALID_PARAMETERS
             *   ERR_OK, release the lock successfully
             *   ERR_TIMEOUT, operation timeout
             */
            virtual void unlock(const std::string& lock_id,
                                p_lockctx ctx,
                                const std::function<void (dsn_error_t ec)>& cb);
        };
    }
}
