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
#pragma once

#include <dsn/dist/distributed_lock_service.h>
#include <unordered_map>
#include "lock_types.h"

namespace dsn { namespace dist {

class zookeeper_session;
class distributed_lock_service_zookeeper: public distributed_lock_service, public clientlet, public ref_counter
{
public:
    explicit distributed_lock_service_zookeeper();
    virtual ~distributed_lock_service_zookeeper();

    // lock_root = argv[0]
    virtual error_code initialize(int argc, const char** argv) override;

    virtual std::pair<task_ptr, task_ptr> lock(
        const std::string& lock_id,
        const std::string& myself_id,
        task_code lock_cb_code,
        const lock_callback& lock_cb,
        task_code lease_expire_code,
        const lock_callback& lease_expire_callback, 
        const lock_options& opt
        ) override;
    virtual task_ptr cancel_pending_lock(
        const std::string& lock_id,
        const std::string& myself_id,
        task_code cb_code,
        const lock_callback& cb) override;
    virtual task_ptr unlock(
        const std::string& lock_id,
        const std::string& myself_id,
        bool destroy,
        task_code cb_code,
        const err_callback& cb) override;
    virtual task_ptr query_lock(
        const std::string& lock_id,
        task_code cb_code,
        const lock_callback& cb) override;
    virtual error_code query_cache(
        const std::string& lock_id, 
        std::string& owner, 
        uint64_t& version);

    void refresh_lock_cache(const std::string& lock_id, const std::string& owner, uint64_t version);
private:
    static std::string LOCK_NODE_PREFIX;

    typedef std::pair<std::string, std::string> lock_key;
    struct pair_hash 
    {
        template<typename T, typename U>
        std::size_t operator ()(const std::pair<T, U>& key) const
        {
            return std::hash<T>()(key.first)+std::hash<U>()(key.second);
        }
    };

    std::string _lock_root; // lock path: ${lock_root}/${lock_id}/${LOCK_NODE_PREFIX}${i}

    typedef std::unordered_map<lock_key, lock_struct_ptr, pair_hash> lock_map;
    typedef std::map<std::string, std::pair<std::string, uint64_t> > cache_map;

    utils::rw_lock_nr _service_lock;
    lock_map _zookeeper_locks;
    cache_map _lock_cache;

    zookeeper_session* _session;
    int _zoo_state;
    bool _first_call;
    utils::notify_event _waiting_attach;

    void erase(const lock_key& key);
    void dispatch_zookeeper_session_expire();
    zookeeper_session* session() { return _session; }

    static void on_zoo_session_evt(lock_srv_ptr _this, int zoo_state);

    friend class lock_struct;
};

}}
