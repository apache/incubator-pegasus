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

#include "table_metrics.h"

#include <fmt/core.h>
#include <string>

#include "utils/fmt_logging.h"
#include "utils/string_view.h"

METRIC_DEFINE_entity(table);

// The number of partitions in each status, see `health_status` and `partition_health_status()`
// for details.

METRIC_DEFINE_gauge_int64(
    table,
    dead_partitions,
    dsn::metric_unit::kPartitions,
    "The number of dead partitions among all tables, which means primary = 0 && secondary = 0");

METRIC_DEFINE_gauge_int64(table,
                          unreadable_partitions,
                          dsn::metric_unit::kPartitions,
                          "The number of unreadable partitions among all tables, which means "
                          "primary = 0 && secondary > 0");

METRIC_DEFINE_gauge_int64(table,
                          unwritable_partitions,
                          dsn::metric_unit::kPartitions,
                          "The number of unwritable partitions among all tables, which means "
                          "primary = 1 && primary + secondary < mutation_2pc_min_replica_count");

METRIC_DEFINE_gauge_int64(table,
                          writable_ill_partitions,
                          dsn::metric_unit::kPartitions,
                          "The number of writable ill partitions among all tables, which means "
                          "primary = 1 && primary + secondary >= mutation_2pc_min_replica_count && "
                          "primary + secondary < max_replica_count");

METRIC_DEFINE_gauge_int64(table,
                          healthy_partitions,
                          dsn::metric_unit::kPartitions,
                          "The number of healthy partitions among all tables, which means primary "
                          "= 1 && primary + secondary >= max_replica_count");

METRIC_DEFINE_counter(table,
                      partition_configuration_changes,
                      dsn::metric_unit::kChanges,
                      "The number of times the configuration has been changed");

METRIC_DEFINE_counter(table,
                      unwritable_partition_changes,
                      dsn::metric_unit::kChanges,
                      "The number of times the status of partition has been changed to unwritable");

METRIC_DEFINE_counter(table,
                      writable_partition_changes,
                      dsn::metric_unit::kChanges,
                      "The number of times the status of partition has been changed to writable");

namespace dsn {

namespace {

metric_entity_ptr instantiate_table_metric_entity(int32_t table_id)
{
    auto entity_id = fmt::format("table_{}", table_id);

    return METRIC_ENTITY_table.instantiate(entity_id, {{"table_id", std::to_string(table_id)}});
}

} // anonymous namespace

table_metrics::table_metrics(int32_t table_id)
    : _table_id(table_id),
      _table_metric_entity(instantiate_table_metric_entity(table_id)),
      METRIC_VAR_INIT_table(dead_partitions),
      METRIC_VAR_INIT_table(unreadable_partitions),
      METRIC_VAR_INIT_table(unwritable_partitions),
      METRIC_VAR_INIT_table(writable_ill_partitions),
      METRIC_VAR_INIT_table(healthy_partitions),
      METRIC_VAR_INIT_table(partition_configuration_changes),
      METRIC_VAR_INIT_table(unwritable_partition_changes),
      METRIC_VAR_INIT_table(writable_partition_changes)
{
}

const metric_entity_ptr &table_metrics::table_metric_entity() const
{
    CHECK_NOTNULL(_table_metric_entity,
                  "table metric entity should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate "
                  "metric");
    return _table_metric_entity;
}

void table_metric_entities::create_entity(int32_t table_id)
{
    utils::auto_write_lock l(_lock);

    entity_map::const_iterator iter = _entities.find(table_id);
    if (dsn_unlikely(iter != _entities.end())) {
        return;
    }

    _entities[table_id] = std::make_unique<table_metrics>(table_id);
}

void table_metric_entities::remove_entity(int32_t table_id)
{
    utils::auto_write_lock l(_lock);

    entity_map::const_iterator iter = _entities.find(table_id);
    if (dsn_unlikely(iter == _entities.end())) {
        return;
    }

    _entities.erase(iter);
}

void table_metric_entities::clear_entities()
{
    utils::auto_write_lock l(_lock);
    _entities.clear();
}

bool operator==(const table_metric_entities &lhs, const table_metric_entities &rhs)
{
    utils::auto_read_lock l1(lhs._lock);
    utils::auto_read_lock l2(rhs._lock);

    if (lhs._entities.size() != rhs._entities.size()) {
        return false;
    }

    for (const auto &lhs_entity : lhs._entities) {
        auto rhs_entity = rhs._entities.find(lhs_entity.first);
        if (rhs_entity == rhs._entities.end()) {
            return false;
        }

        if (lhs_entity.second->table_metric_entity().get() !=
            rhs_entity->second->table_metric_entity().get()) {
            CHECK_NE(lhs_entity.second->table_id(), rhs_entity->second->table_id());
            return false;
        }

        CHECK_EQ(lhs_entity.second->table_id(), rhs_entity->second->table_id());
    }

    return true;
}

} // namespace dsn
