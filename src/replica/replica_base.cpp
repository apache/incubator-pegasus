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

#include "replica_base.h"

#include <string_view>
#include <fmt/core.h>

METRIC_DEFINE_entity(replica);

namespace dsn {
namespace replication {

namespace {

metric_entity_ptr instantiate_replica_metric_entity(const gpid &id)
{
    auto entity_id = fmt::format("replica@{}", id);

    // Do NOT add `replica_base._app_name` as the table name to the attributes of entity, since
    // it is read-only and will never be updated even if the table is renamed.
    return METRIC_ENTITY_replica.instantiate(
        entity_id,
        {{"table_id", std::to_string(id.get_app_id())},
         {"partition_id", std::to_string(id.get_partition_index())}});
}

} // anonymous namespace

replica_base::replica_base(gpid id, std::string_view name, std::string_view app_name)
    : _gpid(id),
      _name(name),
      _app_name(app_name),
      _replica_metric_entity(instantiate_replica_metric_entity(id))
{
}

} // namespace replication
} // namespace dsn
