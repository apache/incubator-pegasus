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
#include <utility>
#include <vector>

#include "duplication_types.h"
#include "nlohmann/detail/json_ref.hpp"
#include "nlohmann/json_fwd.hpp"
#include "utils/config_api.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/singleton.h"
#include "utils/time_utils.h"

namespace dsn {
namespace replication {

DSN_DEFINE_uint32(replication,
                  duplicate_log_batch_bytes,
                  4096,
                  "send mutation log batch bytes size per rpc");
DSN_TAG_VARIABLE(duplicate_log_batch_bytes, FT_MUTABLE);

const std::string duplication_constants::kDuplicationCheckpointRootDir /*NOLINT*/ = "duplication";
const std::string duplication_constants::kClustersSectionName /*NOLINT*/ = "pegasus.clusters";
const std::string duplication_constants::kDuplicationEnvMasterClusterKey /*NOLINT*/ =
    "duplication.master_cluster";
const std::string duplication_constants::kDuplicationEnvMasterMetasKey /*NOLINT*/ =
    "duplication.master_metas";

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

    const std::map<std::string, uint8_t> &get_duplication_group() { return _group; }
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
        nlohmann::json sub_json;
        for (const auto &p : ent.progress) {
            sub_json[std::to_string(p.first)] = p.second;
        }
        json["progress"] = sub_json;
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

/*extern*/ const std::map<std::string, uint8_t> &get_duplication_group()
{
    return internal::duplication_group_registry::instance().get_duplication_group();
}

/*extern*/ const std::set<uint8_t> &get_distinct_cluster_id_set()
{
    return internal::duplication_group_registry::instance().get_distinct_cluster_id_set();
}

} // namespace replication
} // namespace dsn
