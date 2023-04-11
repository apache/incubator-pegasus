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

#include "meta/meta_data.h"
#include "utils/fmt_logging.h"
#include "utils/string_view.h"

METRIC_DEFINE_entity(partition);

METRIC_DEFINE_counter(partition,
                      partition_configuration_changes,
                      dsn::metric_unit::kChanges,
                      "The number of times the configuration has been changed");

METRIC_DEFINE_counter(partition,
                      unwritable_partition_changes,
                      dsn::metric_unit::kChanges,
                      "The number of times the status of partition has been changed to unwritable");

METRIC_DEFINE_counter(partition,
                      writable_partition_changes,
                      dsn::metric_unit::kChanges,
                      "The number of times the status of partition has been changed to writable");

METRIC_DEFINE_gauge_int64(
    partition,
    greedy_recent_balance_operations,
    dsn::metric_unit::kOperations,
    "The number of balance operations of greedy balancer that are recently needed to be executed");

METRIC_DEFINE_counter(partition,
                      greedy_move_primary_operations,
                      dsn::metric_unit::kOperations,
                      "The number of operations that move primaries");

METRIC_DEFINE_counter(partition,
                      greedy_copy_primary_operations,
                      dsn::metric_unit::kOperations,
                      "The number of operations that copy primaries");

METRIC_DEFINE_counter(partition,
                      greedy_copy_secondary_operations,
                      dsn::metric_unit::kOperations,
                      "The number of operations that copy secondaries");

METRIC_DEFINE_entity(table);

// The number of partitions in each status, see `health_status` and `partition_health_status()`
// for details.

METRIC_DEFINE_gauge_int64(
    table,
    dead_partitions,
    dsn::metric_unit::kPartitions,
    "The number of dead partitions, which means primary = 0 && secondary = 0");

METRIC_DEFINE_gauge_int64(table,
                          unreadable_partitions,
                          dsn::metric_unit::kPartitions,
                          "The number of unreadable partitions, which means primary = 0 && "
                          "secondary > 0");

METRIC_DEFINE_gauge_int64(table,
                          unwritable_partitions,
                          dsn::metric_unit::kPartitions,
                          "The number of unwritable partitions, which means primary = 1 && "
                          "primary + secondary < mutation_2pc_min_replica_count");

METRIC_DEFINE_gauge_int64(table,
                          writable_ill_partitions,
                          dsn::metric_unit::kPartitions,
                          "The number of writable ill partitions, which means primary = 1 && "
                          "primary + secondary >= mutation_2pc_min_replica_count && "
                          "primary + secondary < max_replica_count");

METRIC_DEFINE_gauge_int64(table,
                          healthy_partitions,
                          dsn::metric_unit::kPartitions,
                          "The number of healthy partitions, which means primary = 1 && "
                          "primary + secondary >= max_replica_count");

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

metric_entity_ptr instantiate_partition_metric_entity(int32_t table_id, int32_t partition_id)
{
    auto entity_id = fmt::format("partition_{}", gpid(table_id, partition_id));

    return METRIC_ENTITY_partition.instantiate(
        entity_id,
        {{"table_id", std::to_string(table_id)}, {"partition_id", std::to_string(partition_id)}});
}

metric_entity_ptr instantiate_table_metric_entity(int32_t table_id)
{
    auto entity_id = fmt::format("table_{}", table_id);

    return METRIC_ENTITY_table.instantiate(entity_id, {{"table_id", std::to_string(table_id)}});
}

} // anonymous namespace

partition_metrics::partition_metrics(int32_t table_id, int32_t partition_id)
    : _partition_metric_entity(instantiate_partition_metric_entity(table_id, partition_id)),
      METRIC_VAR_INIT_partition(partition_configuration_changes),
      METRIC_VAR_INIT_partition(unwritable_partition_changes),
      METRIC_VAR_INIT_partition(writable_partition_changes)
{
}

const metric_entity_ptr &partition_metrics::partition_metric_entity() const
{
    CHECK_NOTNULL(_partition_metric_entity,
                  "partition metric entity should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate "
                  "metric");
    return _partition_metric_entity;
}

table_metrics::table_metrics(int32_t table_id, int32_t partition_count)
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
    _partition_metrics.reserve(partition_count);
    for (int32_t i = 0; i < partition_count; ++i) {
        _partition_metrics.push_back(std::make_unique<partition_metrics>(table_id, i));
    }
}

const metric_entity_ptr &table_metrics::table_metric_entity() const
{
    CHECK_NOTNULL(_table_metric_entity,
                  "table metric entity should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate "
                  "metric");
    return _table_metric_entity;
}

void table_metrics::resize_partitions(int32_t partition_count)
{
    utils::auto_write_lock l(_partition_lock);

    if (_partition_metrics.size() == partition_count) {
        return;
    }

    if (_partition_metrics.size() > partition_count) {
        _partition_metrics.resize(partition_count);
        return;
    }

    for (int32_t i = _partition_metrics.size(); i < partition_count; ++i) {
        _partition_metrics.push_back(std::make_unique<partition_metrics>(_table_id, i));
    }
}

bool operator==(const table_metrics &lhs, const table_metrics &rhs)
{
    if (&lhs == &rhs) {
        return true;
    }

    if (lhs.table_metric_entity().get() != rhs.table_metric_entity().get()) {
        CHECK_NE(lhs.table_id(), rhs.table_id());
        return false;
    }

    CHECK_EQ(lhs.table_id(), rhs.table_id());
    return true;
}

bool operator!=(const table_metrics &lhs, const table_metrics &rhs) { return !(lhs == rhs); }

void table_metric_entities::create_entity(int32_t table_id, int32_t partition_count)
{
    utils::auto_write_lock l(_lock);

    entity_map::const_iterator iter = _entities.find(table_id);
    if (dsn_unlikely(iter != _entities.end())) {
        return;
    }

    _entities[table_id] = std::make_unique<table_metrics>(table_id, partition_count);
}

void table_metric_entities::resize_partitions(int32_t table_id, int32_t partition_count)
{
    utils::auto_write_lock l(_lock);

    auto iter = _entities.find(table_id);
    if (dsn_unlikely(iter == _entities.end())) {
        return;
    }

    iter->second->resize_partitions(partition_count);
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

void table_metric_entities::set_health_stats(int32_t table_id,
                                             int64_t dead_partitions,
                                             int64_t unreadable_partitions,
                                             int64_t unwritable_partitions,
                                             int64_t writable_ill_partitions,
                                             int64_t healthy_partitions)
{
    utils::auto_read_lock l(_lock);

    auto iter = _entities.find(table_id);
    if (dsn_unlikely(iter == _entities.end())) {
        return;
    }

#define METRIC_SET_HEALTH_STAT(name) METRIC_CALL_SET_METHOD(*(iter->second), name, name)

    METRIC_SET_HEALTH_STAT(dead_partitions);
    METRIC_SET_HEALTH_STAT(unreadable_partitions);
    METRIC_SET_HEALTH_STAT(unwritable_partitions);
    METRIC_SET_HEALTH_STAT(writable_ill_partitions);
    METRIC_SET_HEALTH_STAT(healthy_partitions);

#undef METRIC_SET_HEALTH_STAT
}

bool operator==(const table_metric_entities &lhs, const table_metric_entities &rhs)
{
    if (&lhs == &rhs) {
        return true;
    }

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

        if (*(lhs_entity.second) != *(rhs_entity->second)) {
            return false;
        }
    }

    return true;
}

} // namespace dsn
