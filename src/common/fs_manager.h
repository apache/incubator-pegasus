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

#include <gtest/gtest_prod.h>
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
#include "utils/string_view.h"
#include "utils/metrics.h"
#include "utils/ports.h"
#include "utils/zlocks.h"

namespace dsn {
class gpid;

namespace replication {

DSN_DECLARE_int32(disk_min_available_space_ratio);

class disk_capacity_metrics
{
public:
    disk_capacity_metrics(const std::string &tag, const std::string &data_dir);
    ~disk_capacity_metrics() = default;

    const metric_entity_ptr &disk_metric_entity() const;

    METRIC_DEFINE_SET_METHOD(total_disk_capacity_mb, int64_t)
    METRIC_DEFINE_SET_METHOD(avail_disk_capacity_mb, int64_t)

private:
    const metric_entity_ptr _disk_metric_entity;
    METRIC_VAR_DECLARE_gauge_int64(total_disk_capacity_mb);
    METRIC_VAR_DECLARE_gauge_int64(avail_disk_capacity_mb);

    DISALLOW_COPY_AND_ASSIGN(disk_capacity_metrics);
};

struct dir_node
{
public:
    const std::string tag;
    const std::string full_dir;
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
    // All functions are not thread-safe. However, they are only used in fs_manager
    // and protected by the lock in fs_manager.
    uint64_t replicas_count(app_id id) const;
    uint64_t replicas_count() const;
    // Construct the replica dir for the given 'app_type' and 'pid'.
    // NOTE: Just construct the string, the directory will not be created.
    std::string replica_dir(dsn::string_view app_type, const dsn::gpid &pid) const;
    bool has(const dsn::gpid &pid) const;
    uint64_t remove(const dsn::gpid &pid);
    bool update_disk_stat(const bool update_disk_status);
};

class fs_manager
{
public:
    fs_manager() = default;
    ~fs_manager() = default;

    // Should be called before open/load any replicas.
    // NOTE: 'data_dirs' and 'data_dir_tags' must have the same size and in the same order.
    void initialize(const std::vector<std::string> &data_dirs,
                    const std::vector<std::string> &data_dir_tags);
    // Try to find the best dir_node to place the new replica. The less replica count the
    // dir_node has, which means the load is lower generally, the higher opportunity it
    // will be selected.
    // TODO(yingchun): consider the disk capacity and available space.
    // NOTE: the 'pid' must not exist in any dir_nodes.
    dir_node *find_best_dir_for_new_replica(const dsn::gpid &pid) const;
    dsn::error_code get_disk_tag(const std::string &dir, /*out*/ std::string &tag);
    void add_replica(const dsn::gpid &pid, const std::string &pid_dir);
    // Find the replica instance directory.
    dir_node *find_replica_dir(dsn::string_view app_type, gpid pid);
    // Similar to the above, but it will create a new directory if not found.
    dir_node *create_replica_dir_if_necessary(dsn::string_view app_type, gpid pid);
    // Similar to the above, and will create a directory for the child on the same dir_node
    // of parent.
    // During partition split, we should guarantee child replica and parent replica share the
    // same data dir.
    dir_node *create_child_replica_dir(dsn::string_view app_type,
                                       gpid child_pid,
                                       const std::string &parent_dir);
    void remove_replica(const dsn::gpid &pid);
    bool for_each_dir_node(const std::function<bool(const dir_node &)> &func) const;
    void update_disk_stat(bool check_status_changed = true);

    void add_new_dir_node(const std::string &data_dir, const std::string &tag);
    const std::vector<std::shared_ptr<dir_node>> &get_dir_nodes() const
    {
        zauto_read_lock l(_lock);
        return _dir_nodes;
    }
    bool is_dir_node_available(const std::string &data_dir, const std::string &tag) const;

private:
    dir_node *get_dir_node(const std::string &subdir) const;

    // TODO(wangdan): _dir_nodes should be protected by lock since add_new_disk are supported:
    // it might be updated arbitrarily at any time.
    //
    // Especially when visiting the holding_replicas, you must take care.
    mutable zrwlock_nr _lock;

    int64_t _total_capacity_mb = 0;
    int64_t _total_available_mb = 0;

    std::vector<std::shared_ptr<dir_node>> _dir_nodes;

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
    FRIEND_TEST(fs_manager, find_best_dir_for_new_replica);
    FRIEND_TEST(fs_manager, get_dir_node);
    FRIEND_TEST(open_replica_test, open_replica_add_decree_and_ballot_check);
    FRIEND_TEST(replica_test, test_auto_trash);
};
} // replication
} // dsn
