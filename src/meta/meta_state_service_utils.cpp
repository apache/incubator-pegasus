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

#include "runtime/pipeline.h"
#include "utils/fmt_logging.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"

#include "meta_state_service_utils.h"
#include "meta_state_service_utils_impl.h"

namespace dsn {
namespace replication {
namespace mss {

meta_storage::meta_storage(dist::meta_state_service *remote_storage, task_tracker *tracker)
    : _remote(remote_storage), _tracker(tracker)
{
    CHECK_NOTNULL(tracker, "must set task tracker");
}

meta_storage::~meta_storage() = default;

void meta_storage::create_node_recursively(std::queue<std::string> &&nodes,
                                           blob &&value,
                                           std::function<void()> &&cb)
{
    CHECK(!nodes.empty(), "");

    on_create_recursively op;
    op.initialize(this);
    op.args.reset(
        new on_create_recursively::arguments{std::move(cb), std::move(value), std::move(nodes)});
    op.run();
}

void meta_storage::create_node(std::string &&node, blob &&value, std::function<void()> &&cb)
{
    on_create op;
    op.initialize(this);
    op.args.reset(new on_create::arguments{std::move(cb), std::move(value), std::move(node)});
    op.run();
}

void meta_storage::delete_node_recursively(std::string &&node, std::function<void()> &&cb)
{
    delete_node_impl(std::move(node), std::move(cb), true);
}

void meta_storage::delete_node(std::string &&node, std::function<void()> &&cb)
{
    delete_node_impl(std::move(node), std::move(cb), false);
}

void meta_storage::delete_node_impl(std::string &&node,
                                    std::function<void()> &&cb,
                                    bool is_recursive)
{
    on_delete op;
    op.initialize(this);
    op.args.reset(new on_delete::arguments);
    op.args->cb = std::move(cb);
    op.args->node = std::move(node);
    op.args->is_recursively_delete = is_recursive;
    op.run();
}

void meta_storage::set_data(std::string &&node, blob &&value, std::function<void()> &&cb)
{
    on_set_data op;
    op.initialize(this);
    op.args.reset(new on_set_data::arguments{std::move(cb), std::move(node), std::move(value)});
    op.run();
}

void meta_storage::get_data(std::string &&node, std::function<void(const blob &)> &&cb)
{
    on_get_data op;
    op.initialize(this);
    op.args.reset(new on_get_data::arguments{std::move(cb), std::move(node)});
    op.run();
}

void meta_storage::get_children(std::string &&node,
                                std::function<void(bool, const std::vector<std::string> &)> &&cb)
{
    on_get_children op;
    op.initialize(this);
    op.args.reset(new on_get_children::arguments{std::move(cb), std::move(node)});
    op.run();
}

} // namespace mss
} // namespace replication
} // namespace dsn
