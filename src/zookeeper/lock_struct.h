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
#include <memory>
#include <string>
#include <vector>

#include "lock_types.h"
#include "task/future_types.h"
#include "utils/autoref_ptr.h"
#include "utils/distributed_lock_service.h"
#include "utils/fmt_utils.h"
#include "utils/thread_access_checker.h"

namespace dsn::dist {

enum lock_state
{
    uninitialized = 0,
    pending,
    locked,
    expired,
    cancelled,
    unlocking,
    state_count
};
USER_DEFINED_ENUM_FORMATTER(lock_state)

struct zoolock_pair
{
    zoolock_pair() = default;

    void clear()
    {
        _node_value.clear();
        _node_seq_name.clear();
        _sequence_id = -1;
    }

    std::string _node_value;
    std::string _node_seq_name;
    int64_t _sequence_id{-1};
};

class lock_struct;

using lock_struct_ptr = ref_ptr<lock_struct>;

class lock_struct : public ref_counter
{
public:
    explicit lock_struct(lock_srv_ptr srv);
    void initialize(std::string lock_id, std::string myself_id);
    [[nodiscard]] int hash() const { return _hash; }

    static void
    try_lock(lock_struct_ptr _this, lock_future_ptr lock_callback, lock_future_ptr expire_callback);
    static void cancel_pending_lock(lock_struct_ptr _this, lock_future_ptr cancel_callback);
    static void unlock(lock_struct_ptr _this, error_code_future_ptr unlock_callback);

    static void lock_expired(lock_struct_ptr _this);

private:
    void create_locknode();
    void get_lockdir_nodes();
    void get_lock_owner(bool watch_myself);
    void remove_duplicated_locknode(std::string &&znode_path);

    void remove_my_locknode(std::string &&znode_path, bool ignore_callback, bool remove_for_unlock);

    void clear();
    void remove_lock();
    void on_operation_timeout();
    void on_expire();

    static int64_t parse_seq_path(const std::string &path);
    static void after_create_lockdir(lock_struct_ptr _this, int ec);
    static void after_get_lockdir_nodes(lock_struct_ptr _this,
                                        int ec,
                                        std::shared_ptr<std::vector<std::string>> children);
    static void
    after_create_locknode(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> path);
    static void
    after_get_lock_owner(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> value);
    static void after_self_check(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> value);
    static void after_remove_duplicated_locknode(lock_struct_ptr _this,
                                                 int ec,
                                                 std::shared_ptr<std::string> path);
    static void after_remove_my_locknode(lock_struct_ptr _this, int ec, bool remove_for_unlock);

    /*lock owner watch callback*/
    static void owner_change(lock_struct_ptr _this, int zoo_event);
    static void my_lock_removed(lock_struct_ptr _this, int zoo_event);

    lock_future_ptr _lock_callback;
    lock_future_ptr _lease_expire_callback;
    lock_future_ptr _cancel_callback;
    error_code_future_ptr _unlock_callback;

    std::string _lock_id;
    std::string _lock_dir; // ${lock_root}/${lock_id}
    zoolock_pair _myself;
    zoolock_pair _owner;
    lock_state _state{lock_state::uninitialized};
    int _hash{0};

    lock_srv_ptr _dist_lock_service;

    thread_access_checker _checker;
};

} // namespace dsn::dist
