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

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/container/detail/std_fwd.hpp"
#include "lock_struct.h"
#include "lock_types.h"
#include "task/future_types.h"
#include "task/task.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/distributed_lock_service.h"
#include "utils/error_code.h"
#include "utils/ports.h"
#include "utils/synchronize.h"

namespace dsn::dist {

class zookeeper_session;

class distributed_lock_service_zookeeper : public distributed_lock_service, public ref_counter
{
public:
    distributed_lock_service_zookeeper() = default;
    ~distributed_lock_service_zookeeper() override;

    // lock_root = argv[0]
    error_code initialize(const std::vector<std::string> &args) override;
    error_code finalize() override;

    //
    // distributed lock service implemented by zk.
    // lock_cb is called when get the lock
    //
    // lease_expire_callback is called when the session-expire's zk-event is encountered
    // use should exist the process when lease expires
    //
    std::pair<task_ptr, task_ptr> lock(const std::string &lock_id,
                                       const std::string &myself_id,
                                       task_code lock_cb_code,
                                       const lock_callback &lock_cb,
                                       task_code lease_expire_code,
                                       const lock_callback &lease_expire_callback,
                                       const lock_options &opt) override;

    task_ptr cancel_pending_lock(const std::string &lock_id,
                                 const std::string &myself_id,
                                 task_code cb_code,
                                 const lock_callback &cb) override;
    task_ptr unlock(const std::string &lock_id,
                    const std::string &myself_id,
                    bool destroy,
                    task_code cb_code,
                    const err_callback &cb) override;
    task_ptr
    query_lock(const std::string &lock_id, task_code cb_code, const lock_callback &cb) override;
    error_code query_cache(const std::string &lock_id,
                           /*out*/ std::string &owner,
                           /*out*/ uint64_t &version) const override;

    void refresh_lock_cache(const std::string &lock_id, const std::string &owner, uint64_t version);

private:
    static std::string LOCK_NODE_PREFIX;

    typedef std::pair<std::string, std::string> lock_key;
    struct pair_hash
    {
        template <typename T, typename U>
        std::size_t operator()(const std::pair<T, U> &key) const
        {
            return std::hash<T>()(key.first) + std::hash<U>()(key.second);
        }
    };

    std::string _lock_root; // lock path: ${lock_root}/${lock_id}/${LOCK_NODE_PREFIX}${i}

    using lock_map = std::unordered_map<lock_key, lock_struct_ptr, pair_hash>;
    using cache_map = std::map<std::string, std::pair<std::string, uint64_t>>;

    mutable utils::rw_lock_nr _service_lock;
    lock_map _zookeeper_locks;
    cache_map _lock_cache;

    zookeeper_session *_session{nullptr};
    int _zoo_state{0};
    bool _first_call{true};
    utils::notify_event _waiting_attach;

    void erase(const lock_key &key);
    void dispatch_zookeeper_session_expire();
    zookeeper_session *session() { return _session; }

    static void on_zoo_session_evt(lock_srv_ptr _this, int zoo_state);

    friend class lock_struct;

    DISALLOW_COPY_AND_ASSIGN(distributed_lock_service_zookeeper);
    DISALLOW_MOVE_AND_ASSIGN(distributed_lock_service_zookeeper);
};

} // namespace dsn::dist
