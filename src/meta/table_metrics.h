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

    const metric_entity_ptr &partition_metric_entity() const;

    METRIC_DEFINE_INCREMENT_METHOD(partition_configuration_changes)
    METRIC_DEFINE_INCREMENT_METHOD(unwritable_partition_changes)
    METRIC_DEFINE_INCREMENT_METHOD(writable_partition_changes)
    METRIC_DEFINE_SET_METHOD(greedy_recent_balance_operations, int64_t)

private:
    const metric_entity_ptr _partition_metric_entity;
    METRIC_VAR_DECLARE_counter(partition_configuration_changes);
    METRIC_VAR_DECLARE_counter(unwritable_partition_changes);
    METRIC_VAR_DECLARE_counter(writable_partition_changes);

    DISALLOW_COPY_AND_ASSIGN(partition_metrics);
};

// Maintain a table-level metric entity of meta, and all metrics attached to it.
class table_metrics
{
public:
    table_metrics(int32_t table_id, int32_t partition_count);
    ~table_metrics() = default;

    inline int32_t table_id() const { return _table_id; }
    const metric_entity_ptr &table_metric_entity() const;

    void resize_partitions(int32_t partition_count);

    METRIC_DEFINE_SET_METHOD(dead_partitions, int64_t)
    METRIC_DEFINE_SET_METHOD(unreadable_partitions, int64_t)
    METRIC_DEFINE_SET_METHOD(unwritable_partitions, int64_t)
    METRIC_DEFINE_SET_METHOD(writable_ill_partitions, int64_t)
    METRIC_DEFINE_SET_METHOD(healthy_partitions, int64_t)
    METRIC_DEFINE_INCREMENT_METHOD(partition_configuration_changes)
    METRIC_DEFINE_INCREMENT_METHOD(unwritable_partition_changes)
    METRIC_DEFINE_INCREMENT_METHOD(writable_partition_changes)

private:
    const int32_t _table_id;

    const metric_entity_ptr _table_metric_entity;
    METRIC_VAR_DECLARE_gauge_int64(dead_partitions);
    METRIC_VAR_DECLARE_gauge_int64(unreadable_partitions);
    METRIC_VAR_DECLARE_gauge_int64(unwritable_partitions);
    METRIC_VAR_DECLARE_gauge_int64(writable_ill_partitions);
    METRIC_VAR_DECLARE_gauge_int64(healthy_partitions);
    METRIC_VAR_DECLARE_counter(partition_configuration_changes);
    METRIC_VAR_DECLARE_counter(unwritable_partition_changes);
    METRIC_VAR_DECLARE_counter(writable_partition_changes);

    mutable utils::rw_lock_nr _partition_lock;
    std::vector<std::unique_ptr<partition_metrics>> _partition_metrics;

    DISALLOW_COPY_AND_ASSIGN(table_metrics);
};

bool operator==(const table_metrics &lhs, const table_metrics &rhs);
bool operator!=(const table_metrics &lhs, const table_metrics &rhs);

#define METRIC_DEFINE_TABLE_SET_METHOD(name, value_type)                                           \
    void set_##name(int32_t table_id, value_type value)                                            \
    {                                                                                              \
        utils::auto_read_lock l(_lock);                                                            \
                                                                                                   \
        entity_map::const_iterator iter = _entities.find(table_id);                                \
        if (dsn_unlikely(iter == _entities.end())) {                                               \
            return;                                                                                \
        }                                                                                          \
        METRIC_CALL_SET_METHOD(*(iter->second), name, value);                                      \
    }

#define METRIC_CALL_TABLE_SET_METHOD(obj, name, table_id, value) (obj).set_##name(table_id, value)

#define METRIC_DEFINE_TABLE_INCREMENT_METHOD(name)                                                 \
    void increment_##name(int32_t table_id)                                                        \
    {                                                                                              \
        utils::auto_read_lock l(_lock);                                                            \
                                                                                                   \
        entity_map::const_iterator iter = _entities.find(table_id);                                \
        if (dsn_unlikely(iter == _entities.end())) {                                               \
            return;                                                                                \
        }                                                                                          \
        METRIC_CALL_INCREMENT_METHOD(*(iter->second), name);                                       \
    }

#define METRIC_CALL_TABLE_INCREMENT_METHOD(obj, name, table_id) (obj).increment_##name(table_id)

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

    METRIC_DEFINE_TABLE_INCREMENT_METHOD(partition_configuration_changes)
    METRIC_DEFINE_TABLE_INCREMENT_METHOD(unwritable_partition_changes)
    METRIC_DEFINE_TABLE_INCREMENT_METHOD(writable_partition_changes)

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
