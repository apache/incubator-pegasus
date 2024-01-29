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

#pragma once

#include <stdint.h>
#include <list>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "runtime/task/future_types.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/distributed_lock_service.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
namespace dist {

// A simple version of distributed lock service.
// NOTE: Only for test purpose.
class distributed_lock_service_simple : public distributed_lock_service
{
public:
    virtual ~distributed_lock_service_simple() { _tracker.cancel_outstanding_tasks(); }
    // no parameter need
    virtual error_code initialize(const std::vector<std::string> &args) override;
    virtual error_code finalize() override { return ERR_OK; }

    virtual std::pair<task_ptr, task_ptr> lock(const std::string &lock_id,
                                               const std::string &myself_id,
                                               task_code lock_cb_code,
                                               const lock_callback &lock_cb,
                                               task_code lease_expire_code,
                                               const lock_callback &lease_expire_callback,
                                               const lock_options &opt) override;

    virtual task_ptr cancel_pending_lock(const std::string &lock_id,
                                         const std::string &myself_id,
                                         task_code cb_code,
                                         const lock_callback &cb) override;

    virtual task_ptr unlock(const std::string &lock_id,
                            const std::string &myself_id,
                            bool destroy,
                            task_code cb_code,
                            const err_callback &cb) override;

    virtual task_ptr
    query_lock(const std::string &lock_id, task_code cb_code, const lock_callback &cb) override;

    virtual error_code query_cache(const std::string &lock_id,
                                   /*out*/ std::string &owner,
                                   /*out*/ uint64_t &version) override;

private:
    void random_lock_lease_expire(const std::string &lock_id);

private:
    struct lock_wait_info
    {
        task_ptr grant_callback;
        task_ptr lease_callback;
        std::string owner;
    };

    struct lock_info
    {
        std::string owner;
        uint64_t version;
        task_ptr lease_callback;
        std::list<lock_wait_info> pending_list;
    };

    typedef std::unordered_map<std::string, lock_info> locks;

    zlock _lock;
    locks _dlocks; // lock -> owner

    dsn::task_tracker _tracker;
};
}
}
