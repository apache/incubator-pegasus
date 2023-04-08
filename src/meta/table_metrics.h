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

#include "utils/metrics.h"

namespace dsn {

class table_metrics
{
public:
    table_metrics(int32_t table_id);
    ~table_metrics() = default;

    const metric_entity_ptr &table_metric_entity() const;

    METRIC_DEFINE_SET_METHOD(dead_partitions, int64_t)
    METRIC_DEFINE_SET_METHOD(unreadable_partitions, int64_t)
    METRIC_DEFINE_SET_METHOD(unwritable_partitions, int64_t)
    METRIC_DEFINE_SET_METHOD(writable_ill_partitions, int64_t)
    METRIC_DEFINE_SET_METHOD(healthy_partitions, int64_t)
    METRIC_DEFINE_INCREMENT_METHOD(partition_config_updates)
    METRIC_DEFINE_INCREMENT_METHOD(unwritable_partition_updates)
    METRIC_DEFINE_INCREMENT_METHOD(writable_partition_updates)

private:
    const int32_t _table_id;

    const metric_entity_ptr _table_metric_entity;
    METRIC_VAR_DECLARE_gauge_int64(dead_partitions);
    METRIC_VAR_DECLARE_gauge_int64(unreadable_partitions);
    METRIC_VAR_DECLARE_gauge_int64(unwritable_partitions);
    METRIC_VAR_DECLARE_gauge_int64(writable_ill_partitions);
    METRIC_VAR_DECLARE_gauge_int64(healthy_partitions);
    METRIC_VAR_DECLARE_counter(partition_config_updates);
    METRIC_VAR_DECLARE_counter(unwritable_partition_updates);
    METRIC_VAR_DECLARE_counter(writable_partition_updates);

    DISALLOW_COPY_AND_ASSIGN(table_metrics);
};

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

class table_metric_entities
{
public:
    using entity_map = std::unordered_map<int, std::unique_ptr<table_metrics>>;

    table_metric_entities() = default;
    ~table_metric_entities() = default;

    void create_entity(int32_t table_id);
    void remove_entity(int32_t table_id);
    void clear_entities();

    METRIC_DEFINE_TABLE_SET_METHOD(dead_partitions, int64_t)
    METRIC_DEFINE_TABLE_SET_METHOD(unreadable_partitions, int64_t)
    METRIC_DEFINE_TABLE_SET_METHOD(unwritable_partitions, int64_t)
    METRIC_DEFINE_TABLE_SET_METHOD(writable_ill_partitions, int64_t)
    METRIC_DEFINE_TABLE_SET_METHOD(healthy_partitions, int64_t)
    METRIC_DEFINE_TABLE_INCREMENT_METHOD(partition_config_updates)
    METRIC_DEFINE_TABLE_INCREMENT_METHOD(unwritable_partition_updates)
    METRIC_DEFINE_TABLE_INCREMENT_METHOD(writable_partition_updates)

private:
    mutable utils::rw_lock_nr _lock;
    entity_map _entities;

    DISALLOW_COPY_AND_ASSIGN(table_metric_entities);
};

} // namespace dsn
