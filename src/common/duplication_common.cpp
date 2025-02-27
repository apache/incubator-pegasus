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

#include "duplication_common.h"

#include <nlohmann/json.hpp>
#include <cstdint>
#include <map>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/common.h"
#include "duplication_types.h"
#include "nlohmann/detail/json_ref.hpp"
#include "nlohmann/json_fwd.hpp"
#include "utils/config_api.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/singleton.h"
#include "utils/time_utils.h"

DSN_DEFINE_uint32(replication,
                  duplicate_log_batch_bytes,
                  4096,
                  "send mutation log batch bytes size per rpc");
DSN_TAG_VARIABLE(duplicate_log_batch_bytes, FT_MUTABLE);

// While many clusters are duplicated to a target cluster, we have to add many cluster
// ids to the `*.ini` file of the target cluster, and the target cluster might be restarted
// very frequently.
//
// This option is added so that only the target cluster id should be configured while
// there is no need to add any other cluster id.
DSN_DEFINE_bool(replication,
                dup_ignore_other_cluster_ids,
                false,
                "Allow any other cluster id except myself to be ignored for duplication");

namespace dsn {
namespace replication {

const std::string duplication_constants::kDuplicationCheckpointRootDir /*NOLINT*/ = "duplication";
const std::string duplication_constants::kClustersSectionName /*NOLINT*/ = "pegasus.clusters";
const std::string duplication_constants::kEnvMasterClusterKey /*NOLINT*/ =
    "duplication.master_cluster";
const std::string duplication_constants::kEnvMasterMetasKey /*NOLINT*/ = "duplication.master_metas";
const std::string duplication_constants::kEnvMasterAppNameKey /*NOLINT*/ =
    "duplication.master_app_name";
const std::string duplication_constants::kEnvFollowerAppStatusKey /*NOLINT*/
    = "duplication.follower_app_status";
const std::string duplication_constants::kEnvFollowerAppStatusCreating /*NOLINT*/
    = "creating";
const std::string duplication_constants::kEnvFollowerAppStatusCreated /*NOLINT*/
    = "created";

/*extern*/ const char *duplication_status_to_string(duplication_status::type status)
{
    auto it = _duplication_status_VALUES_TO_NAMES.find(status);
    CHECK(it != _duplication_status_VALUES_TO_NAMES.end(),
          "unexpected type of duplication_status: {}",
          status);
    return it->second;
}

/*extern*/ const char *duplication_fail_mode_to_string(duplication_fail_mode::type fmode)
{
    auto it = _duplication_fail_mode_VALUES_TO_NAMES.find(fmode);
    CHECK(it != _duplication_fail_mode_VALUES_TO_NAMES.end(),
          "unexpected type of duplication_fail_mode: {}",
          fmode);
    return it->second;
}

namespace internal {

class duplication_group_registry : public utils::singleton<duplication_group_registry>
{
public:
    error_with<uint8_t> get_cluster_id(const std::string &cluster_name) const
    {
        if (cluster_name.empty()) {
            return error_s::make(ERR_INVALID_PARAMETERS, "cluster_name is empty");
        }
        if (_group.empty()) {
            return error_s::make(ERR_OBJECT_NOT_FOUND, "`duplication-group` is not configured");
        }

        auto it = _group.find(cluster_name);
        if (it == _group.end()) {
            return error_s::make(ERR_OBJECT_NOT_FOUND, "failed to get cluster id for ")
                   << cluster_name.data();
        }
        return it->second;
    }

    const std::set<uint8_t> &get_distinct_cluster_id_set() { return _distinct_cids; }

private:
    duplication_group_registry()
    {
        std::vector<std::string> clusters;
        dsn_config_get_all_keys("duplication-group", clusters);
        for (std::string &cluster : clusters) {
            int64_t cluster_id =
                dsn_config_get_value_int64("duplication-group", cluster.data(), 0, "");
            CHECK(cluster_id < 128 && cluster_id > 0,
                  "cluster_id({}) for {} should be in [1, 127]",
                  cluster_id,
                  cluster);
            _group.emplace(cluster, static_cast<uint8_t>(cluster_id));
        }
        CHECK_EQ_MSG(clusters.size(),
                     _group.size(),
                     "there might be duplicate cluster_name in configuration");

        // TODO(yingchun): add InsertValuesFromMap to src/gutil/map_util.h, then use it here.
        for (const auto &kv : _group) {
            _distinct_cids.insert(kv.second);
        }
        CHECK_EQ_MSG(_distinct_cids.size(),
                     _group.size(),
                     "there might be duplicate cluster_id in configuration");
    }
    ~duplication_group_registry() = default;

    std::map<std::string, uint8_t> _group;
    std::set<uint8_t> _distinct_cids;

    friend class utils::singleton<duplication_group_registry>;
};

} // namespace internal

/*extern*/ error_with<uint8_t> get_duplication_cluster_id(const std::string &cluster_name)
{
    return internal::duplication_group_registry::instance().get_cluster_id(cluster_name);
}

/*extern*/ uint8_t get_current_dup_cluster_id_or_default()
{
    // Set cluster id to 0 as default if it is not configured, which means it would accept
    // writes from any cluster as long as the timestamp is larger.
    static const auto res = get_duplication_cluster_id(get_current_dup_cluster_name());
    static const uint8_t cluster_id = res.is_ok() ? res.get_value() : 0;
    return cluster_id;
}

/*extern*/ uint8_t get_current_dup_cluster_id()
{
    static const uint8_t cluster_id =
        get_duplication_cluster_id(get_current_dup_cluster_name()).get_value();
    return cluster_id;
}

// TODO(wutao1): implement our C++ version of `TSimpleJSONProtocol` if there're
//               more cases for converting thrift to JSON
static nlohmann::json duplication_entry_to_json(const duplication_entry &ent)
{
    char ts_buf[30];
    utils::time_ms_to_date_time(static_cast<uint64_t>(ent.create_ts), ts_buf, sizeof(ts_buf));
    nlohmann::json json{
        {"dupid", ent.dupid},
        {"create_ts", ts_buf},
        {"remote", ent.remote},
        {"status", duplication_status_to_string(ent.status)},
        {"fail_mode", duplication_fail_mode_to_string(ent.fail_mode)},
    };

    if (ent.__isset.progress) {
        nlohmann::json progress;
        for (const auto &[partition_index, state] : ent.progress) {
            progress[std::to_string(partition_index)] = state;
        }

        json["progress"] = progress;
    }

    if (ent.__isset.remote_app_name) {
        // remote_app_name is supported since v2.6.0, thus it won't be shown before v2.6.0.
        json["remote_app_name"] = ent.remote_app_name;
    }

    if (ent.__isset.remote_replica_count) {
        // remote_replica_count is supported since v2.6.0, thus it won't be shown before v2.6.0.
        json["remote_replica_count"] = ent.remote_replica_count;
    }

    if (ent.__isset.partition_states) {
        nlohmann::json partition_states;
        for (const auto &[partition_index, state] : ent.partition_states) {
            nlohmann::json partition_state;
            partition_state["confirmed_decree"] = state.confirmed_decree;
            partition_state["last_committed_decree"] = state.last_committed_decree;

            partition_states[std::to_string(partition_index)] = partition_state;
        }

        json["partition_states"] = partition_states;
    }

    return json;
}

/*extern*/ std::string duplication_entry_to_string(const duplication_entry &ent)
{
    return duplication_entry_to_json(ent).dump();
}

/*extern*/ std::string duplication_query_response_to_string(const duplication_query_response &resp)
{
    nlohmann::json json;
    int i = 1;
    for (const auto &ent : resp.entry_list) {
        json["appid"] = resp.appid;
        json[std::to_string(i)] = duplication_entry_to_json(ent);
        i++;
    }
    return json.dump();
}

/*extern*/ const std::set<uint8_t> &get_distinct_cluster_id_set()
{
    return internal::duplication_group_registry::instance().get_distinct_cluster_id_set();
}

/*extern*/ bool is_dup_cluster_id_configured(uint8_t cluster_id)
{
    if (cluster_id != get_current_dup_cluster_id()) {
        if (FLAGS_dup_ignore_other_cluster_ids) {
            return true;
        }
    }

    return get_distinct_cluster_id_set().find(cluster_id) != get_distinct_cluster_id_set().end();
}

} // namespace replication
} // namespace dsn
