/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     fs_manager's implement: used to track the disk position for all the allocated replicas
 *
 * Revision history:
 *     2017-08-08: sunweijie@xiaomi.com, first draft
 */

#include "fs_manager.h"
#include "utils/utils.h"
#include "utils/filesystem.h"
#include <thread>
#include "utils/fmt_logging.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

DSN_DEFINE_int32("replication",
                 disk_min_available_space_ratio,
                 10,
                 "if disk available space ratio "
                 "is below this value, this "
                 "disk will be considered as "
                 "space insufficient");
DSN_TAG_VARIABLE(disk_min_available_space_ratio, FT_MUTABLE);

unsigned dir_node::replicas_count() const
{
    unsigned sum = 0;
    for (const auto &s : holding_replicas) {
        sum += s.second.size();
    }
    return sum;
}

unsigned dir_node::replicas_count(app_id id) const
{
    const auto iter = holding_replicas.find(id);
    if (iter == holding_replicas.end())
        return 0;
    return iter->second.size();
}

bool dir_node::has(const gpid &pid) const
{
    auto iter = holding_replicas.find(pid.get_app_id());
    if (iter == holding_replicas.end())
        return false;
    return iter->second.find(pid) != iter->second.end();
}

unsigned dir_node::remove(const gpid &pid)
{
    auto iter = holding_replicas.find(pid.get_app_id());
    if (iter == holding_replicas.end())
        return 0;
    return iter->second.erase(pid);
}

bool dir_node::update_disk_stat(const bool update_disk_status)
{
    FAIL_POINT_INJECT_F("update_disk_stat", [](string_view) { return false; });
    dsn::utils::filesystem::disk_space_info info;
    if (!dsn::utils::filesystem::get_disk_space_info(full_dir, info)) {
        LOG_ERROR_F("update disk space failed: dir = {}", full_dir);
        return false;
    }
    // update disk space info
    disk_capacity_mb = info.capacity / 1024 / 1024;
    disk_available_mb = info.available / 1024 / 1024;
    disk_available_ratio = static_cast<int>(
        disk_capacity_mb == 0 ? 0 : std::round(disk_available_mb * 100.0 / disk_capacity_mb));

    if (!update_disk_status) {
        LOG_INFO_F("update disk space succeed: dir = {}, capacity_mb = {}, available_mb = {}, "
                   "available_ratio = {}%",
                   full_dir,
                   disk_capacity_mb,
                   disk_available_mb,
                   disk_available_ratio);
        return false;
    }
    auto old_status = status;
    auto new_status = disk_available_ratio < FLAGS_disk_min_available_space_ratio
                          ? disk_status::SPACE_INSUFFICIENT
                          : disk_status::NORMAL;
    if (old_status != new_status) {
        status = new_status;
    }
    LOG_INFO_F("update disk space succeed: dir = {}, capacity_mb = {}, available_mb = {}, "
               "available_ratio = {}%, disk_status = {}",
               full_dir,
               disk_capacity_mb,
               disk_available_mb,
               disk_available_ratio,
               enum_to_string(status));
    return (old_status != new_status);
}

fs_manager::fs_manager(bool for_test)
{
    if (!for_test) {
        _counter_total_capacity_mb.init_app_counter("eon.replica_stub",
                                                    "disk.capacity.total(MB)",
                                                    COUNTER_TYPE_NUMBER,
                                                    "total disk capacity in MB");
        _counter_total_available_mb.init_app_counter("eon.replica_stub",
                                                     "disk.available.total(MB)",
                                                     COUNTER_TYPE_NUMBER,
                                                     "total disk available in MB");
        _counter_total_available_ratio.init_app_counter("eon.replica_stub",
                                                        "disk.available.total.ratio",
                                                        COUNTER_TYPE_NUMBER,
                                                        "total disk available ratio");
        _counter_min_available_ratio.init_app_counter("eon.replica_stub",
                                                      "disk.available.min.ratio",
                                                      COUNTER_TYPE_NUMBER,
                                                      "minimal disk available ratio in all disks");
        _counter_max_available_ratio.init_app_counter("eon.replica_stub",
                                                      "disk.available.max.ratio",
                                                      COUNTER_TYPE_NUMBER,
                                                      "maximal disk available ratio in all disks");
    }
}

dir_node *fs_manager::get_dir_node(const std::string &subdir)
{
    std::string norm_subdir;
    utils::filesystem::get_normalized_path(subdir, norm_subdir);
    for (auto &n : _dir_nodes) {
        // if input is a subdir of some dir_nodes
        const std::string &d = n->full_dir;
        if (norm_subdir.compare(0, d.size(), d) == 0 &&
            (norm_subdir.size() == d.size() || norm_subdir[d.size()] == '/')) {
            return n.get();
        }
    }
    return nullptr;
}

// size of the two vectors should be equal
dsn::error_code fs_manager::initialize(const std::vector<std::string> &data_dirs,
                                       const std::vector<std::string> &tags,
                                       bool for_test)
{
    // create all dir_nodes
    CHECK_EQ(data_dirs.size(), tags.size());
    for (unsigned i = 0; i < data_dirs.size(); ++i) {
        std::string norm_path;
        utils::filesystem::get_normalized_path(data_dirs[i], norm_path);
        dir_node *n = new dir_node(tags[i], norm_path);
        _dir_nodes.emplace_back(n);
        LOG_INFO("%s: mark data dir(%s) as tag(%s)",
                 dsn_primary_address().to_string(),
                 norm_path.c_str(),
                 tags[i].c_str());
    }
    _available_data_dirs = data_dirs;

    if (!for_test) {
        update_disk_stat(false);
    }
    return dsn::ERR_OK;
}

dsn::error_code fs_manager::get_disk_tag(const std::string &dir, std::string &tag)
{
    dir_node *n = get_dir_node(dir);
    if (nullptr == n) {
        return dsn::ERR_OBJECT_NOT_FOUND;
    } else {
        tag = n->tag;
        return dsn::ERR_OK;
    }
}

void fs_manager::add_replica(const gpid &pid, const std::string &pid_dir)
{
    dir_node *n = get_dir_node(pid_dir);
    if (nullptr == n) {
        LOG_ERROR("%s: dir(%s) of gpid(%d.%d) haven't registered",
                  dsn_primary_address().to_string(),
                  pid_dir.c_str(),
                  pid.get_app_id(),
                  pid.get_partition_index());
    } else {
        zauto_write_lock l(_lock);
        std::set<dsn::gpid> &replicas_for_app = n->holding_replicas[pid.get_app_id()];
        auto result = replicas_for_app.emplace(pid);
        if (!result.second) {
            LOG_WARNING("%s: gpid(%d.%d) already in the dir_node(%s)",
                        dsn_primary_address().to_string(),
                        pid.get_app_id(),
                        pid.get_partition_index(),
                        n->tag.c_str());
        } else {
            LOG_INFO("%s: add gpid(%d.%d) to dir_node(%s)",
                     dsn_primary_address().to_string(),
                     pid.get_app_id(),
                     pid.get_partition_index(),
                     n->tag.c_str());
        }
    }
}

void fs_manager::allocate_dir(const gpid &pid, const std::string &type, /*out*/ std::string &dir)
{
    char buffer[256];
    sprintf(buffer, "%d.%d.%s", pid.get_app_id(), pid.get_partition_index(), type.c_str());

    zauto_write_lock l(_lock);

    dir_node *selected = nullptr;

    unsigned least_app_replicas_count = 0;
    unsigned least_total_replicas_count = 0;

    for (auto &n : _dir_nodes) {
        CHECK(!n->has(pid), "gpid({}) already in dir_node({})", pid, n->tag);
        unsigned app_replicas = n->replicas_count(pid.get_app_id());
        unsigned total_replicas = n->replicas_count();

        if (selected == nullptr || least_app_replicas_count > app_replicas) {
            least_app_replicas_count = app_replicas;
            least_total_replicas_count = total_replicas;
            selected = n.get();
        } else if (least_app_replicas_count == app_replicas &&
                   least_total_replicas_count > total_replicas) {
            least_total_replicas_count = total_replicas;
            selected = n.get();
        }
    }

    LOG_INFO(
        "%s: put pid(%d.%d) to dir(%s), which has %u replicas of current app, %u replicas totally",
        dsn_primary_address().to_string(),
        pid.get_app_id(),
        pid.get_partition_index(),
        selected->tag.c_str(),
        least_app_replicas_count,
        least_total_replicas_count);

    selected->holding_replicas[pid.get_app_id()].emplace(pid);
    dir = utils::filesystem::path_combine(selected->full_dir, buffer);
}

void fs_manager::remove_replica(const gpid &pid)
{
    zauto_write_lock l(_lock);
    unsigned remove_count = 0;
    for (auto &n : _dir_nodes) {
        unsigned r = n->remove(pid);
        CHECK_LE_MSG(remove_count + r,
                     1,
                     "gpid({}) found in dir({}), which was removed before",
                     pid,
                     n->tag);
        if (r != 0) {
            LOG_INFO("%s: remove gpid(%d.%d) from dir(%s)",
                     dsn_primary_address().to_string(),
                     pid.get_app_id(),
                     pid.get_partition_index(),
                     n->tag.c_str());
        }
        remove_count += r;
    }
}

bool fs_manager::for_each_dir_node(const std::function<bool(const dir_node &)> &func) const
{
    zauto_read_lock l(_lock);
    for (auto &n : _dir_nodes) {
        if (!func(*n))
            return false;
    }
    return true;
}

void fs_manager::update_disk_stat(bool check_status_changed)
{
    reset_disk_stat();
    for (auto &dir_node : _dir_nodes) {
        if (dir_node->update_disk_stat(check_status_changed)) {
            _status_updated_dir_nodes.emplace_back(dir_node);
        }
        _total_capacity_mb += dir_node->disk_capacity_mb;
        _total_available_mb += dir_node->disk_available_mb;
        _min_available_ratio = std::min(dir_node->disk_available_ratio, _min_available_ratio);
        _max_available_ratio = std::max(dir_node->disk_available_ratio, _max_available_ratio);
    }
    _total_available_ratio = static_cast<int>(
        _total_capacity_mb == 0 ? 0 : std::round(_total_available_mb * 100.0 / _total_capacity_mb));

    LOG_INFO_F("update disk space succeed: disk_count = {}, total_capacity_mb = {}, "
               "total_available_mb = {}, total_available_ratio = {}%, min_available_ratio = {}%, "
               "max_available_ratio = {}%",
               _dir_nodes.size(),
               _total_capacity_mb,
               _total_available_mb,
               _total_available_ratio,
               _min_available_ratio,
               _max_available_ratio);
    _counter_total_capacity_mb->set(_total_capacity_mb);
    _counter_total_available_mb->set(_total_available_mb);
    _counter_total_available_ratio->set(_total_available_ratio);
    _counter_min_available_ratio->set(_min_available_ratio);
    _counter_max_available_ratio->set(_max_available_ratio);
}

void fs_manager::add_new_dir_node(const std::string &data_dir, const std::string &tag)
{
    zauto_write_lock l(_lock);
    std::string norm_path;
    utils::filesystem::get_normalized_path(data_dir, norm_path);
    dir_node *n = new dir_node(tag, norm_path);
    _dir_nodes.emplace_back(n);
    _available_data_dirs.emplace_back(data_dir);
    LOG_INFO_F(
        "{}: mark data dir({}) as tag({})", dsn_primary_address().to_string(), norm_path, tag);
}

bool fs_manager::is_dir_node_available(const std::string &data_dir, const std::string &tag) const
{
    zauto_read_lock l(_lock);
    for (const auto &dir_node : _dir_nodes) {
        std::string norm_path;
        utils::filesystem::get_normalized_path(data_dir, norm_path);
        if (dir_node->full_dir == norm_path || dir_node->tag == tag) {
            return true;
        }
    }
    return false;
}

} // namespace replication
} // namespace dsn
