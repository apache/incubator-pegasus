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
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/replication_other_types.h"
#include "metadata_types.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/metrics.h"
#include "utils/ports.h"
#include "utils/zlocks.h"

namespace dsn {
class gpid;

namespace replication {
class replication_options;

DSN_DECLARE_int32(disk_min_available_space_ratio);

class disk_capacity_metrics
{
public:
    disk_capacity_metrics(const std::string &tag, const std::string &data_dir);
    ~disk_capacity_metrics() = default;

    const metric_entity_ptr &disk_metric_entity() const;

    METRIC_DEFINE_SET_METHOD(total_disk_capacity_mb, int64_t)
    METRIC_DEFINE_SET_METHOD(avail_disk_capacity_mb, int64_t)
    METRIC_DEFINE_SET_METHOD(avail_disk_capacity_percentage, int64_t)

private:
    const metric_entity_ptr _disk_metric_entity;
    METRIC_VAR_DECLARE_gauge_int64(total_disk_capacity_mb);
    METRIC_VAR_DECLARE_gauge_int64(avail_disk_capacity_mb);
    METRIC_VAR_DECLARE_gauge_int64(avail_disk_capacity_percentage);

    DISALLOW_COPY_AND_ASSIGN(disk_capacity_metrics);
};

struct dir_node
{
public:
    std::string tag;
    std::string full_dir;
    int64_t disk_capacity_mb;
    int64_t disk_available_mb;
    int disk_available_ratio;
    disk_status::type status;
    std::map<app_id, std::set<gpid>> holding_replicas;
    std::map<app_id, std::set<gpid>> holding_primary_replicas;
    std::map<app_id, std::set<gpid>> holding_secondary_replicas;

private:
    disk_capacity_metrics disk_capacity;

public:
    dir_node(const std::string &tag_,
             const std::string &dir_,
             int64_t disk_capacity_mb_ = 0,
             int64_t disk_available_mb_ = 0,
             int disk_available_ratio_ = 0,
             disk_status::type status_ = disk_status::NORMAL)
        : tag(tag_),
          full_dir(dir_),
          disk_capacity_mb(disk_capacity_mb_),
          disk_available_mb(disk_available_mb_),
          disk_available_ratio(disk_available_ratio_),
          status(status_),
          disk_capacity(tag_, dir_)
    {
    }
    unsigned replicas_count(app_id id) const;
    unsigned replicas_count() const;
    bool has(const dsn::gpid &pid) const;
    unsigned remove(const dsn::gpid &pid);
    bool update_disk_stat(const bool update_disk_status);
};

class fs_manager
{
public:
    fs_manager() = default;
    ~fs_manager() = default;

    // this should be called before open/load any replicas
    dsn::error_code initialize(const replication_options &opts);
    dsn::error_code initialize(const std::vector<std::string> &data_dirs,
                               const std::vector<std::string> &tags,
                               bool for_test);

    dsn::error_code get_disk_tag(const std::string &dir, /*out*/ std::string &tag);
    void allocate_dir(const dsn::gpid &pid,
                      const std::string &type,
                      /*out*/ std::string &dir);
    void add_replica(const dsn::gpid &pid, const std::string &pid_dir);
    void remove_replica(const dsn::gpid &pid);
    bool for_each_dir_node(const std::function<bool(const dir_node &)> &func) const;
    void update_disk_stat(bool check_status_changed = true);

    void add_new_dir_node(const std::string &data_dir, const std::string &tag);
    bool is_dir_node_available(const std::string &data_dir, const std::string &tag) const;
    const std::vector<std::string> &get_available_data_dirs() const
    {
        zauto_read_lock l(_lock);
        return _available_data_dirs;
    }

private:
    dir_node *get_dir_node(const std::string &subdir);

    // TODO(wangdan): _dir_nodes should be protected by lock since add_new_disk are supported:
    // it might be updated arbitrarily at any time.
    //
    // Especially when visiting the holding_replicas, you must take care.
    mutable zrwlock_nr _lock;

    int64_t _total_capacity_mb = 0;
    int64_t _total_available_mb = 0;

    std::vector<std::shared_ptr<dir_node>> _dir_nodes;
    std::vector<std::string> _available_data_dirs;

    // Used for disk available space check
    // disk status will be updated periodically, this vector record nodes whose disk_status changed
    // in this round
    std::vector<std::shared_ptr<dir_node>> _status_updated_dir_nodes;

    friend class replica_test;
    friend class replica_stub;
    friend class mock_replica_stub;
    friend class replica_disk_migrator;
    friend class replica_disk_test_base;
    friend class open_replica_test;
};
} // replication
} // dsn
