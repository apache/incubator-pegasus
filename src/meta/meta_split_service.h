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
#include "meta_service.h"
#include "server_state.h"

namespace dsn {
namespace replication {

class meta_split_service
{
public:
    explicit meta_split_service(meta_service *meta);

private:
    // client -> meta to start split
    void start_partition_split(start_split_rpc rpc);
    void do_start_partition_split(std::shared_ptr<app_state> app, start_split_rpc rpc);

    // client -> meta to query split
    void query_partition_split(query_split_rpc rpc) const;

    // client -> meta to pause/restart/cancel split
    void control_partition_split(control_split_rpc rpc);

    // pause/restart specific one partition
    void do_control_single(std::shared_ptr<app_state> app, control_split_rpc rpc);

    // pause all splitting partitions or restart all paused partitions or cancel all partitions
    void do_control_all(std::shared_ptr<app_state> app, control_split_rpc rpc);

    // primary parent -> meta_server to register child
    void register_child_on_meta(register_child_rpc rpc);

    // meta -> remote storage to update child replica config
    dsn::task_ptr add_child_on_remote_storage(register_child_rpc rpc, bool create_new);
    void
    on_add_child_on_remote_storage_reply(error_code ec, register_child_rpc rpc, bool create_new);

    // primary replica -> meta to notify group pause or cancel split succeed
    void notify_stop_split(notify_stop_split_rpc rpc);
    void do_cancel_partition_split(std::shared_ptr<app_state> app, notify_stop_split_rpc rpc);

    static const std::string control_type_str(split_control_type::type type)
    {
        std::string str = "";
        if (type == split_control_type::PAUSE) {
            str = "pause";
        } else if (type == split_control_type::RESTART) {
            str = "restart";
        } else if (type == split_control_type::CANCEL) {
            str = "cancel";
        }
        return str;
    }

private:
    friend class meta_service;
    friend class meta_split_service_test;

    meta_service *_meta_svc;
    server_state *_state;

    zrwlock_nr &app_lock() const { return _state->_lock; }
};
} // namespace replication
} // namespace dsn
