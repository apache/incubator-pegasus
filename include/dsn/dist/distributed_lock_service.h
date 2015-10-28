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
# include <memory>
# include "service_supplier.h"

namespace dsn
{
    namespace dist
    {
        class distributed_lock_service
        {
        public:
            typedef void* lock_handle;
            template<typename T>
            static distributed_lock_service* open_service(std::shared_ptr<service_supplier> supplier)
            {
                return T::open(supplier);
            }

            virtual lock_handle create_lock(const std::string& lock_name);
            /*
             * try to get the lock:
             *     lock_name: the global lock_id
             *     myself: the name of myself. If someone got the lock
             *       service_supplier use it to notify others who got the
             *       lock
             *     lock_callback:
                     (1) if try_lock error, the lock_callback is called, and
                         user can get the error reason by ec
                     (2) if the caller don't get the lock, the lock_callback is called.
                         In this case, ec should be 0, and caller could know who got the
                         lock by the value of "who"
                     (3) if the caller get the lock, the lock_callback is called
                         and who==myself
             */
            virtual void try_lock(lock_handle handle,
                                  const std::string& myself,
                                  const std::function<void (int ec, std::string&& who)>& lock_callback) = 0;
            virtual void free_lock(lock_handle handle) = 0;
        };
    }
}
