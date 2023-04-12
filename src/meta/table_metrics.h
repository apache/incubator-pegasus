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

#include "common/gpid.h"
#include "utils/autoref_ptr.h"
#include "utils/metrics.h"
#include "utils/ports.h"
#include "utils/synchronize.h"

namespace dsn {
class table_metric_entities;

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

private:
    const int32_t _table_id;
    const int32_t _partition_id;

    const metric_entity_ptr _partition_metric_entity;
    METRIC_VAR_DECLARE_counter(partition_configuration_changes);
    METRIC_VAR_DECLARE_counter(unwritable_partition_changes);
    METRIC_VAR_DECLARE_counter(writable_partition_changes);

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

#define __METRIC_DEFINE_INCREMENT(name)                                                            \
    void increment_##name(int32_t partition_id)                                                    \
    {                                                                                              \
        CHECK_LT(partition_id, _partition_metrics.size());                                         \
        METRIC_INCREMENT(*(_partition_metrics[partition_id]), name);                               \
    }

    __METRIC_DEFINE_INCREMENT(partition_configuration_changes)
    __METRIC_DEFINE_INCREMENT(unwritable_partition_changes)
    __METRIC_DEFINE_INCREMENT(writable_partition_changes)

#undef __METRIC_DEFINE_INCREMENT

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

    void set_health_stats(int32_t table_id,
                          int64_t dead_partitions,
                          int64_t unreadable_partitions,
                          int64_t unwritable_partitions,
                          int64_t writable_ill_partitions,
                          int64_t healthy_partitions);

#define __METRIC_DEFINE_INCREMENT(name)                                                            \
    void increment_##name(const gpid &id)                                                          \
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

#undef __METRIC_DEFINE_INCREMENT

private:
    friend bool operator==(const table_metric_entities &, const table_metric_entities &);

    mutable utils::rw_lock_nr _lock;
    entity_map _entities;

    DISALLOW_COPY_AND_ASSIGN(table_metric_entities);
};

bool operator==(const table_metric_entities &lhs, const table_metric_entities &rhs);

#define METRIC_SET_TABLE_HEALTH_STATS(obj, table_id, ...)                                          \
    (obj).set_health_stats(table_id, ##__VA_ARGS__)

} // namespace dsn
