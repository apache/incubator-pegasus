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

#pragma once

#include <stdint.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/ports.h"
#include "utils/synchronize.h"

namespace dsn {

// Maintain a partition-level metric entity of meta, and all metrics attached to it.
class partition_metrics
{
public:
    partition_metrics(int32_t table_id, int32_t partition_id);
    ~partition_metrics() = default;

    inline int32_t table_id() const { return _table_id; }
    inline int32_t partition_id() const { return _partition_id; }
    const metric_entity_ptr &partition_metric_entity() const;

    METRIC_DEFINE_INCREMENT(partition_configuration_changes)
    METRIC_DEFINE_INCREMENT(unwritable_partition_changes)
    METRIC_DEFINE_INCREMENT(writable_partition_changes)

    METRIC_DEFINE_SET(greedy_recent_balance_operations, int64_t)
    METRIC_DEFINE_INCREMENT_BY(greedy_move_primary_operations)
    METRIC_DEFINE_INCREMENT_BY(greedy_copy_primary_operations)
    METRIC_DEFINE_INCREMENT_BY(greedy_copy_secondary_operations)

    METRIC_DEFINE_INCREMENT(choose_primary_failed_operations)

private:
    const int32_t _table_id;
    const int32_t _partition_id;

    const metric_entity_ptr _partition_metric_entity;
    METRIC_VAR_DECLARE_counter(partition_configuration_changes);
    METRIC_VAR_DECLARE_counter(unwritable_partition_changes);
    METRIC_VAR_DECLARE_counter(writable_partition_changes);
    METRIC_VAR_DECLARE_gauge_int64(greedy_recent_balance_operations);
    METRIC_VAR_DECLARE_counter(greedy_move_primary_operations);
    METRIC_VAR_DECLARE_counter(greedy_copy_primary_operations);
    METRIC_VAR_DECLARE_counter(greedy_copy_secondary_operations);
    METRIC_VAR_DECLARE_counter(choose_primary_failed_operations);

    DISALLOW_COPY_AND_ASSIGN(partition_metrics);
};

bool operator==(const partition_metrics &lhs, const partition_metrics &rhs);
bool operator!=(const partition_metrics &lhs, const partition_metrics &rhs);

// Maintain a table-level metric entity of meta, and all metrics attached to it.
class table_metrics
{
public:
    table_metrics(int32_t table_id, int32_t partition_count);
    ~table_metrics() = default;

    inline int32_t table_id() const { return _table_id; }
    const metric_entity_ptr &table_metric_entity() const;

    void resize_partitions(int32_t partition_count);

    METRIC_DEFINE_SET(dead_partitions, int64_t)
    METRIC_DEFINE_SET(unreadable_partitions, int64_t)
    METRIC_DEFINE_SET(unwritable_partitions, int64_t)
    METRIC_DEFINE_SET(writable_ill_partitions, int64_t)
    METRIC_DEFINE_SET(healthy_partitions, int64_t)

#define __METRIC_DEFINE_INCREMENT_BY(name)                                                         \
    void METRIC_FUNC_NAME_INCREMENT_BY(name)(int32_t partition_id, int64_t x)                      \
    {                                                                                              \
        CHECK_LT(partition_id, _partition_metrics.size());                                         \
        METRIC_INCREMENT_BY(*(_partition_metrics[partition_id]), name, x);                         \
    }

    __METRIC_DEFINE_INCREMENT_BY(greedy_move_primary_operations)
    __METRIC_DEFINE_INCREMENT_BY(greedy_copy_primary_operations)
    __METRIC_DEFINE_INCREMENT_BY(greedy_copy_secondary_operations)

#undef __METRIC_DEFINE_INCREMENT_BY

#define __METRIC_DEFINE_INCREMENT(name)                                                            \
    void METRIC_FUNC_NAME_INCREMENT(name)(int32_t partition_id)                                    \
    {                                                                                              \
        CHECK_LT(partition_id, _partition_metrics.size());                                         \
        METRIC_INCREMENT(*(_partition_metrics[partition_id]), name);                               \
    }

    __METRIC_DEFINE_INCREMENT(partition_configuration_changes)
    __METRIC_DEFINE_INCREMENT(unwritable_partition_changes)
    __METRIC_DEFINE_INCREMENT(writable_partition_changes)
    __METRIC_DEFINE_INCREMENT(choose_primary_failed_operations)

#undef __METRIC_DEFINE_INCREMENT

#define __METRIC_DEFINE_SET(name, value_type)                                                      \
    void METRIC_FUNC_NAME_SET(name)(int32_t partition_id, value_type value)                        \
    {                                                                                              \
        CHECK_LT(partition_id, _partition_metrics.size());                                         \
        METRIC_SET(*(_partition_metrics[partition_id]), name, value);                              \
    }

    __METRIC_DEFINE_SET(greedy_recent_balance_operations, int64_t)

#undef __METRIC_DEFINE_SET

private:
    friend bool operator==(const table_metrics &, const table_metrics &);

    const int32_t _table_id;

    const metric_entity_ptr _table_metric_entity;
    METRIC_VAR_DECLARE_gauge_int64(dead_partitions);
    METRIC_VAR_DECLARE_gauge_int64(unreadable_partitions);
    METRIC_VAR_DECLARE_gauge_int64(unwritable_partitions);
    METRIC_VAR_DECLARE_gauge_int64(writable_ill_partitions);
    METRIC_VAR_DECLARE_gauge_int64(healthy_partitions);

    std::vector<std::unique_ptr<partition_metrics>> _partition_metrics;

    DISALLOW_COPY_AND_ASSIGN(table_metrics);
};

bool operator==(const table_metrics &lhs, const table_metrics &rhs);
bool operator!=(const table_metrics &lhs, const table_metrics &rhs);

class greedy_balance_stats
{
public:
    greedy_balance_stats() = default;
    ~greedy_balance_stats() = default;

    struct partition_stats
    {
        int greedy_recent_balance_operations = 0;
        int greedy_move_primary_operations = 0;
        int greedy_copy_primary_operations = 0;
        int greedy_copy_secondary_operations = 0;
    };

    using partition_map = std::unordered_map<gpid, partition_stats>;

#define __METRIC_DEFINE_INCREMENT(name)                                                            \
    void METRIC_FUNC_NAME_INCREMENT(name)(const gpid &id, bool balance_checker)                    \
    {                                                                                              \
        auto &partition = _partition_map[id];                                                      \
        ++(partition.greedy_recent_balance_operations);                                            \
        if (balance_checker) {                                                                     \
            return;                                                                                \
        }                                                                                          \
        ++(partition.name);                                                                        \
    }

    __METRIC_DEFINE_INCREMENT(greedy_move_primary_operations)
    __METRIC_DEFINE_INCREMENT(greedy_copy_primary_operations)
    __METRIC_DEFINE_INCREMENT(greedy_copy_secondary_operations)

#undef __METRIC_DEFINE_INCREMENT

    const partition_map &stats() const { return _partition_map; }

private:
    partition_map _partition_map;

    DISALLOW_COPY_AND_ASSIGN(greedy_balance_stats);
};

// Manage the lifetime of all table-level metric entities of meta.
//
// To instantiate a new table-level entity, just call create_entity(). Once the entity instance
// is not needed, just call remove_entity() (after `entity_retirement_delay_ms` milliseconds it
// would be retired).
class table_metric_entities
{
public:
    using entity_map = std::unordered_map<int, std::unique_ptr<table_metrics>>;

    table_metric_entities() = default;
    ~table_metric_entities() = default;

    void create_entity(int32_t table_id, int32_t partition_count);
    void resize_partitions(int32_t table_id, int32_t partition_count);
    void remove_entity(int32_t table_id);
    void clear_entities();

#define __METRIC_DEFINE_INCREMENT(name)                                                            \
    void METRIC_FUNC_NAME_INCREMENT(name)(const gpid &id)                                          \
    {                                                                                              \
        utils::auto_read_lock l(_lock);                                                            \
                                                                                                   \
        auto iter = _entities.find(id.get_app_id());                                               \
        if (dsn_unlikely(iter == _entities.end())) {                                               \
            return;                                                                                \
        }                                                                                          \
                                                                                                   \
        METRIC_INCREMENT(*(iter->second), name, id.get_partition_index());                         \
    }

    __METRIC_DEFINE_INCREMENT(partition_configuration_changes)
    __METRIC_DEFINE_INCREMENT(unwritable_partition_changes)
    __METRIC_DEFINE_INCREMENT(writable_partition_changes)
    __METRIC_DEFINE_INCREMENT(choose_primary_failed_operations)

#undef __METRIC_DEFINE_INCREMENT

    void set_greedy_balance_stats(const greedy_balance_stats &balance_stats);

    void set_health_stats(int32_t table_id,
                          int dead_partitions,
                          int unreadable_partitions,
                          int unwritable_partitions,
                          int writable_ill_partitions,
                          int healthy_partitions);

private:
    friend bool operator==(const table_metric_entities &, const table_metric_entities &);

    mutable utils::rw_lock_nr _lock;
    entity_map _entities;

    DISALLOW_COPY_AND_ASSIGN(table_metric_entities);
};

bool operator==(const table_metric_entities &lhs, const table_metric_entities &rhs);

#define METRIC_SET_GREEDY_BALANCE_STATS(obj, ...) (obj).set_greedy_balance_stats(__VA_ARGS__)

#define METRIC_SET_TABLE_HEALTH_STATS(obj, table_id, ...)                                          \
    (obj).set_health_stats(table_id, ##__VA_ARGS__)

} // namespace dsn
