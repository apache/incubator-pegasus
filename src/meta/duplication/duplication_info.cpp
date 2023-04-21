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

#include "duplication_info.h"

#include "common/duplication_common.h"
#include "meta/meta_data.h"
#include "runtime/api_layer1.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace replication {

/*extern*/ void json_encode(dsn::json::JsonWriter &out, const duplication_status::type &s)
{
    json::json_encode(out, duplication_status_to_string(s));
}

/*extern*/ bool json_decode(const dsn::json::JsonObject &in, duplication_status::type &s)
{
    static const std::map<std::string, duplication_status::type>
        _duplication_status_NAMES_TO_VALUES = {
            {"DS_INIT", duplication_status::DS_INIT},
            {"DS_PREPARE", duplication_status::DS_PREPARE},
            {"DS_APP", duplication_status::DS_APP},
            {"DS_LOG", duplication_status::DS_LOG},
            {"DS_PAUSE", duplication_status::DS_PAUSE},
            {"DS_REMOVED", duplication_status::DS_REMOVED},
        };

    std::string name;
    json::json_decode(in, name);
    auto it = _duplication_status_NAMES_TO_VALUES.find(name);
    if (it != _duplication_status_NAMES_TO_VALUES.end()) {
        s = it->second;
        return true;
    }
    LOG_ERROR("unexpected duplication_status name: {}", name);

    // for forward compatibility issue, duplication of unexpected status
    // will be marked as invisible.
    s = duplication_status::DS_REMOVED;
    return false;
}

/*extern*/ void json_encode(dsn::json::JsonWriter &out, const duplication_fail_mode::type &fmode)
{
    json::json_encode(out, duplication_fail_mode_to_string(fmode));
}

/*extern*/ bool json_decode(const dsn::json::JsonObject &in, duplication_fail_mode::type &fmode)
{
    static const std::map<std::string, duplication_fail_mode::type>
        _duplication_fail_mode_NAMES_TO_VALUES = {
            {"FAIL_SLOW", duplication_fail_mode::FAIL_SLOW},
            {"FAIL_SKIP", duplication_fail_mode::FAIL_SKIP},
            {"FAIL_FAST", duplication_fail_mode::FAIL_FAST},
        };

    std::string name;
    json::json_decode(in, name);
    auto it = _duplication_fail_mode_NAMES_TO_VALUES.find(name);
    if (it != _duplication_fail_mode_NAMES_TO_VALUES.end()) {
        fmode = it->second;
        return true;
    }
    LOG_ERROR("unexpected duplication_fail_mode name: {}", name);
    // marked as default value.
    fmode = duplication_fail_mode::FAIL_SLOW;
    return false;
}

// lock held
error_code duplication_info::alter_status(duplication_status::type to_status,
                                          duplication_fail_mode::type to_fail_mode)
{
    if (_is_altering) {
        return ERR_BUSY;
    }

    if (_status == duplication_status::DS_REMOVED) {
        return ERR_OBJECT_NOT_FOUND;
    }

    if (!is_valid_alteration(to_status)) {
        return ERR_INVALID_PARAMETERS;
    }

    if (_status == to_status && _fail_mode == to_fail_mode) {
        return ERR_OK;
    }

    zauto_write_lock l(_lock);
    _is_altering = true;
    _next_status = to_status;
    _next_fail_mode = to_fail_mode;
    return ERR_OK;
}

void duplication_info::init_progress(int partition_index, decree d)
{
    zauto_write_lock l(_lock);

    auto &p = _progress[partition_index];
    p.volatile_decree = p.stored_decree = d;
    p.is_inited = true;
}

bool duplication_info::alter_progress(int partition_index,
                                      const duplication_confirm_entry &confirm_entry)
{
    zauto_write_lock l(_lock);

    partition_progress &p = _progress[partition_index];
    if (!p.is_inited) {
        return false;
    }
    if (p.is_altering) {
        return false;
    }

    p.checkpoint_prepared = confirm_entry.checkpoint_prepared;
    if (p.volatile_decree < confirm_entry.confirmed_decree) {
        p.volatile_decree = confirm_entry.confirmed_decree;
    }
    if (p.volatile_decree != p.stored_decree) {
        // progress update is not supposed to be too frequent.
        if (dsn_now_ms() > p.last_progress_update_ms + PROGRESS_UPDATE_PERIOD_MS) {
            p.is_altering = true;
            p.last_progress_update_ms = dsn_now_ms();
            return true;
        }
    }
    return false;
}

void duplication_info::persist_progress(int partition_index)
{
    zauto_write_lock l(_lock);

    auto &p = _progress[partition_index];
    CHECK_PREFIX_MSG(p.is_altering, "partition_index: {}", partition_index);
    p.is_altering = false;
    p.stored_decree = p.volatile_decree;
}

void duplication_info::persist_status()
{
    zauto_write_lock l(_lock);

    if (!_is_altering) {
        LOG_ERROR_PREFIX("callers never write a duplication that is not altering to meta store");
        return;
    }
    LOG_INFO_PREFIX("change duplication status from {} to {} successfully [app_id: {}]",
                    duplication_status_to_string(_status),
                    duplication_status_to_string(_next_status),
                    app_id);

    _is_altering = false;
    _status = _next_status;
    _next_status = duplication_status::DS_INIT;
    _fail_mode = _next_fail_mode;
}

std::string duplication_info::to_string() const
{
    return duplication_entry_to_string(to_duplication_entry());
}

blob duplication_info::to_json_blob() const
{
    json_helper copy;
    copy.create_timestamp_ms = create_timestamp_ms;
    copy.remote = follower_cluster_name;
    copy.status = _next_status;
    copy.fail_mode = _next_fail_mode;
    return json::json_forwarder<json_helper>::encode(copy);
}

void duplication_info::report_progress_if_time_up()
{
    // progress report is not supposed to be too frequent.
    if (dsn_now_ms() > _last_progress_report_ms + PROGRESS_REPORT_PERIOD_MS) {
        _last_progress_report_ms = dsn_now_ms();
        LOG_INFO("duplication report: {}", to_string());
    }
}

duplication_info_s_ptr duplication_info::decode_from_blob(dupid_t dup_id,
                                                          int32_t app_id,
                                                          const std::string &app_name,
                                                          int32_t partition_count,
                                                          std::string store_path,
                                                          const blob &json)
{
    json_helper info;
    if (!json::json_forwarder<json_helper>::decode(json, info)) {
        return nullptr;
    }
    std::vector<rpc_address> meta_list;
    if (!dsn::replication::replica_helper::load_meta_servers(
            meta_list, duplication_constants::kClustersSectionName.c_str(), info.remote.c_str())) {
        return nullptr;
    }

    auto dup = std::make_shared<duplication_info>(dup_id,
                                                  app_id,
                                                  app_name,
                                                  partition_count,
                                                  info.create_timestamp_ms,
                                                  std::move(info.remote),
                                                  std::move(meta_list),
                                                  std::move(store_path));
    dup->_status = info.status;
    dup->_fail_mode = info.fail_mode;
    return dup;
}

void duplication_info::append_if_valid_for_query(
    const app_state &app,
    /*out*/ std::vector<duplication_entry> &entry_list) const
{
    zauto_read_lock l(_lock);

    entry_list.emplace_back(to_duplication_entry());
    duplication_entry &ent = entry_list.back();
    // the confirmed decree is not useful for displaying
    // the overall state of duplication
    ent.__isset.progress = false;
}

} // namespace replication
} // namespace dsn
