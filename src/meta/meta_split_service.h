// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include <string>

#include "common/partition_split_common.h"
#include "partition_split_types.h"
#include "runtime/task/task.h"
#include "server_state.h"

namespace dsn {
class error_code;
class zrwlock_nr;

namespace replication {
class app_state;
class meta_service;

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

    // primary replica -> meta to query child state
    void query_child_state(query_child_state_rpc rpc);

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
