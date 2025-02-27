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

#include <string_view>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <stddef.h>
#include <string>

#include "utils/fmt_logging.h"

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
    "The number of balance operations by greedy balancer that are recently needed to be executed");

METRIC_DEFINE_counter(partition,
                      greedy_move_primary_operations,
                      dsn::metric_unit::kOperations,
                      "The number of balance operations by greedy balancer that move primaries");

METRIC_DEFINE_counter(partition,
                      greedy_copy_primary_operations,
                      dsn::metric_unit::kOperations,
                      "The number of balance operations by greedy balancer that copy primaries");

METRIC_DEFINE_counter(partition,
                      greedy_copy_secondary_operations,
                      dsn::metric_unit::kOperations,
                      "The number of balance operations by greedy balancer that copy secondaries");

METRIC_DEFINE_counter(partition,
                      choose_primary_failed_operations,
                      dsn::metric_unit::kOperations,
                      "The number of operations that fail to choose the primary replica");

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

namespace dsn {

namespace {

metric_entity_ptr instantiate_partition_metric_entity(int32_t table_id, int32_t partition_id)
{
    auto entity_id = fmt::format("partition@{}", gpid(table_id, partition_id));

    return METRIC_ENTITY_partition.instantiate(
        entity_id,
        {{"table_id", std::to_string(table_id)}, {"partition_id", std::to_string(partition_id)}});
}

metric_entity_ptr instantiate_table_metric_entity(int32_t table_id)
{
    auto entity_id = fmt::format("table@{}", table_id);

    return METRIC_ENTITY_table.instantiate(entity_id, {{"table_id", std::to_string(table_id)}});
}

} // anonymous namespace

partition_metrics::partition_metrics(int32_t table_id, int32_t partition_id)
    : _table_id(table_id),
      _partition_id(partition_id),
      _partition_metric_entity(instantiate_partition_metric_entity(table_id, partition_id)),
      METRIC_VAR_INIT_partition(partition_configuration_changes),
      METRIC_VAR_INIT_partition(unwritable_partition_changes),
      METRIC_VAR_INIT_partition(writable_partition_changes),
      METRIC_VAR_INIT_partition(greedy_recent_balance_operations),
      METRIC_VAR_INIT_partition(greedy_move_primary_operations),
      METRIC_VAR_INIT_partition(greedy_copy_primary_operations),
      METRIC_VAR_INIT_partition(greedy_copy_secondary_operations),
      METRIC_VAR_INIT_partition(choose_primary_failed_operations)
{
}

const metric_entity_ptr &partition_metrics::partition_metric_entity() const
{
    CHECK_NOTNULL(_partition_metric_entity,
                  "partition metric entity (table_id={}, partition_id={}) should has been "
                  "instantiated: uninitialized entity cannot be used to instantiate metric",
                  _table_id,
                  _partition_id);
    return _partition_metric_entity;
}

bool operator==(const partition_metrics &lhs, const partition_metrics &rhs)
{
    if (&lhs == &rhs) {
        return true;
    }

    if (lhs.partition_metric_entity().get() != rhs.partition_metric_entity().get()) {
        CHECK_TRUE(lhs.table_id() != rhs.table_id() || lhs.partition_id() != rhs.partition_id());
        return false;
    }

    CHECK_EQ(lhs.table_id(), rhs.table_id());
    CHECK_EQ(lhs.partition_id(), rhs.partition_id());
    return true;
}

bool operator!=(const partition_metrics &lhs, const partition_metrics &rhs)
{
    return !(lhs == rhs);
}

table_metrics::table_metrics(int32_t table_id, int32_t partition_count)
    : _table_id(table_id),
      _table_metric_entity(instantiate_table_metric_entity(table_id)),
      METRIC_VAR_INIT_table(dead_partitions),
      METRIC_VAR_INIT_table(unreadable_partitions),
      METRIC_VAR_INIT_table(unwritable_partitions),
      METRIC_VAR_INIT_table(writable_ill_partitions),
      METRIC_VAR_INIT_table(healthy_partitions)
{
    _partition_metrics.reserve(partition_count);
    for (int32_t i = 0; i < partition_count; ++i) {
        _partition_metrics.push_back(std::make_unique<partition_metrics>(table_id, i));
    }
}

const metric_entity_ptr &table_metrics::table_metric_entity() const
{
    CHECK_NOTNULL(_table_metric_entity,
                  "table metric entity (table_id={}) should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate metric",
                  _table_id);
    return _table_metric_entity;
}

void table_metrics::resize_partitions(int32_t partition_count)
{
    LOG_INFO("resize partitions for table_metrics(table_id={}): old_partition_count={}, "
             "new_partition_count={}",
             _table_id,
             _partition_metrics.size(),
             partition_count);

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

    if (lhs._partition_metrics.size() != rhs._partition_metrics.size()) {
        return false;
    }

    for (size_t i = 0; i < lhs._partition_metrics.size(); ++i) {
        if (*(lhs._partition_metrics[i]) != *(rhs._partition_metrics[i])) {
            return false;
        }
    }

    return true;
}

bool operator!=(const table_metrics &lhs, const table_metrics &rhs) { return !(lhs == rhs); }

void table_metric_entities::create_entity(int32_t table_id, int32_t partition_count)
{
    LOG_INFO("try to create entity for table_metric_entities(table_id={}): partition_count={}",
             table_id,
             partition_count);

    utils::auto_write_lock l(_lock);

    entity_map::const_iterator iter = _entities.find(table_id);
    if (dsn_unlikely(iter != _entities.end())) {
        LOG_WARNING("entity has existed for table_metric_entities(table_id={})", table_id);
        return;
    }

    _entities[table_id] = std::make_unique<table_metrics>(table_id, partition_count);
    LOG_INFO("entity has been created for table_metric_entities(table_id={}): partition_count={}",
             table_id,
             partition_count);
}

void table_metric_entities::resize_partitions(int32_t table_id, int32_t partition_count)
{
    LOG_INFO(
        "try to resize partitions for table_metric_entities(table_id={}): new_partition_count={}",
        table_id,
        partition_count);

    utils::auto_write_lock l(_lock);

    auto iter = _entities.find(table_id);
    if (dsn_unlikely(iter == _entities.end())) {
        LOG_WARNING("entity does not exist for table_metric_entities(table_id={})", table_id);
        return;
    }

    iter->second->resize_partitions(partition_count);
}

void table_metric_entities::remove_entity(int32_t table_id)
{
    LOG_INFO("try to remove entity for table_metric_entities(table_id={})", table_id);

    utils::auto_write_lock l(_lock);

    entity_map::const_iterator iter = _entities.find(table_id);
    if (dsn_unlikely(iter == _entities.end())) {
        LOG_WARNING("entity does not exist for table_metric_entities(table_id={})", table_id);
        return;
    }

    _entities.erase(iter);
    LOG_INFO("entity has been removed for table_metric_entities(table_id={})", table_id);
}

void table_metric_entities::clear_entities()
{
    utils::auto_write_lock l(_lock);
    _entities.clear();
}

void table_metric_entities::set_health_stats(int32_t table_id,
                                             int dead_partitions,
                                             int unreadable_partitions,
                                             int unwritable_partitions,
                                             int writable_ill_partitions,
                                             int healthy_partitions)
{
    utils::auto_read_lock l(_lock);

    auto iter = _entities.find(table_id);
    if (dsn_unlikely(iter == _entities.end())) {
        return;
    }

#define __METRIC_SET(name) METRIC_SET(*(iter->second), name, name)

    __METRIC_SET(dead_partitions);
    __METRIC_SET(unreadable_partitions);
    __METRIC_SET(unwritable_partitions);
    __METRIC_SET(writable_ill_partitions);
    __METRIC_SET(healthy_partitions);

#undef __METRIC_SET
}

void table_metric_entities::set_greedy_balance_stats(const greedy_balance_stats &balance_stats)
{
    utils::auto_read_lock l(_lock);

    const auto &stats = balance_stats.stats();
    for (const auto &partition : stats) {
        auto iter = _entities.find(partition.first.get_app_id());
        if (dsn_unlikely(iter == _entities.end())) {
            continue;
        }

        METRIC_SET(*(iter->second),
                   greedy_recent_balance_operations,
                   partition.first.get_partition_index(),
                   partition.second.greedy_recent_balance_operations);

#define __METRIC_INCREMENT_BY(name)                                                                \
    METRIC_INCREMENT_BY(                                                                           \
        *(iter->second), name, partition.first.get_partition_index(), partition.second.name)

        __METRIC_INCREMENT_BY(greedy_move_primary_operations);
        __METRIC_INCREMENT_BY(greedy_copy_primary_operations);
        __METRIC_INCREMENT_BY(greedy_copy_secondary_operations);

#undef __METRIC_INCREMENT_BY
    }
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
