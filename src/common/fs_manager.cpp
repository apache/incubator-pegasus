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

#include <algorithm>
#include <cmath>
#include <iosfwd>
#include <utility>

#include "common/gpid.h"
#include "common/replication_enums.h"
#include "fmt/core.h"
#include "fmt/ostream.h"
#include "perf_counter/perf_counter.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/string_view.h"

namespace dsn {
namespace replication {

DSN_DEFINE_int32(replication,
                 disk_min_available_space_ratio,
                 10,
                 "if disk available space ratio "
                 "is below this value, this "
                 "disk will be considered as "
                 "space insufficient");
DSN_TAG_VARIABLE(disk_min_available_space_ratio, FT_MUTABLE);

DSN_DEFINE_bool(replication,
                ignore_broken_disk,
                true,
                "true means ignore broken data disk when initialize");

uint64_t dir_node::replicas_count() const
{
    uint64_t sum = 0;
    for (const auto &s : holding_replicas) {
        sum += s.second.size();
    }
    return sum;
}

uint64_t dir_node::replicas_count(app_id id) const
{
    const auto iter = holding_replicas.find(id);
    if (iter == holding_replicas.end()) {
        return 0;
    }
    return iter->second.size();
}

std::string dir_node::replica_dir(dsn::string_view app_type, const dsn::gpid &pid) const
{
    return utils::filesystem::path_combine(full_dir, fmt::format("{}.{}", pid, app_type));
}

bool dir_node::has(const gpid &pid) const
{
    auto iter = holding_replicas.find(pid.get_app_id());
    if (iter == holding_replicas.end()) {
        return false;
    }
    return iter->second.find(pid) != iter->second.end();
}

uint64_t dir_node::remove(const gpid &pid)
{
    auto iter = holding_replicas.find(pid.get_app_id());
    if (iter == holding_replicas.end()) {
        return 0;
    }
    return iter->second.erase(pid);
}

void dir_node::update_disk_stat()
{
    FAIL_POINT_INJECT_F("update_disk_stat", [](string_view) { return; });

    dsn::utils::filesystem::disk_space_info dsi;
    if (!dsn::utils::filesystem::get_disk_space_info(full_dir, dsi)) {
        // TODO(yingchun): it may encounter some IO errors when get_disk_space_info() failed, deal
        //  with it.
        LOG_ERROR("get disk space info failed, dir = {}", full_dir);
        return;
    }

    disk_capacity_mb = dsi.capacity >> 20;
    disk_available_mb = dsi.available >> 20;
    disk_available_ratio = static_cast<int>(
        disk_capacity_mb == 0 ? 0 : std::round(disk_available_mb * 100.0 / disk_capacity_mb));

    auto old_status = status;
    auto new_status = disk_available_ratio < FLAGS_disk_min_available_space_ratio
                          ? disk_status::SPACE_INSUFFICIENT
                          : disk_status::NORMAL;
    if (old_status != new_status) {
        status = new_status;
    }
    LOG_INFO("update disk space succeed: dir = {}, capacity_mb = {}, available_mb = {}, "
             "available_ratio = {}%, disk_status = {}",
             full_dir,
             disk_capacity_mb,
             disk_available_mb,
             disk_available_ratio,
             enum_to_string(status));
}

fs_manager::fs_manager()
{
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

dir_node *fs_manager::get_dir_node(const std::string &subdir) const
{
    std::string norm_subdir;
    utils::filesystem::get_normalized_path(subdir, norm_subdir);

    zauto_read_lock l(_lock);
    for (const auto &dn : _dir_nodes) {
        // Check if 'subdir' is a sub-directory of 'dn'.
        const std::string &full_dir = dn->full_dir;
        if (full_dir.size() > norm_subdir.size()) {
            continue;
        }

        if ((norm_subdir.size() == full_dir.size() || norm_subdir[full_dir.size()] == '/') &&
            norm_subdir.compare(0, full_dir.size(), full_dir) == 0) {
            return dn.get();
        }
    }
    return nullptr;
}

void fs_manager::initialize(const std::vector<std::string> &data_dirs,
                            const std::vector<std::string> &data_dir_tags)
{
    CHECK_EQ(data_dirs.size(), data_dir_tags.size());

    // Skip the data directories which are broken.
    std::vector<std::shared_ptr<dir_node>> dir_nodes;
    for (auto i = 0; i < data_dir_tags.size(); ++i) {
        const auto &dir_tag = data_dir_tags[i];
        const auto &dir = data_dirs[i];

        // Check the status of this directory.
        std::string cdir;
        std::string err_msg;
        if (dsn_unlikely(!utils::filesystem::create_directory(dir, cdir, err_msg) ||
                         !utils::filesystem::check_dir_rw(dir, err_msg))) {
            if (FLAGS_ignore_broken_disk) {
                LOG_ERROR("data dir({}) is broken, ignore it, error: {}", dir, err_msg);
            } else {
                CHECK(false, err_msg);
            }
            // TODO(yingchun): Remove the 'continue' and mark its io error status, regardless
            //  the status of the disks, add all disks.
            continue;
        }

        // Normalize the data directories.
        std::string norm_path;
        utils::filesystem::get_normalized_path(cdir, norm_path);

        // Create and add this dir_node.
        auto dn = std::make_shared<dir_node>(dir_tag, norm_path);
        dir_nodes.emplace_back(dn);
        LOG_INFO("mark data dir({}) as tag({})", norm_path, dir_tag);
    }
    CHECK_FALSE(dir_nodes.empty());

    // Update the memory state.
    {
        zauto_read_lock l(_lock);
        _dir_nodes.swap(dir_nodes);
    }

    // Update the disk statistics.
    update_disk_stat();
}

void fs_manager::add_replica(const gpid &pid, const std::string &pid_dir)
{
    const auto &dn = get_dir_node(pid_dir);
    if (dsn_unlikely(nullptr == dn)) {
        LOG_ERROR(
            "{}: dir({}) of gpid({}) haven't registered", dsn_primary_address(), pid_dir, pid);
        return;
    }

    bool emplace_success = false;
    {
        zauto_write_lock l(_lock);
        auto &replicas_for_app = dn->holding_replicas[pid.get_app_id()];
        emplace_success = replicas_for_app.emplace(pid).second;
    }
    if (!emplace_success) {
        LOG_WARNING(
            "{}: gpid({}) already in the dir_node({})", dsn_primary_address(), pid, dn->tag);
        return;
    }

    LOG_INFO("{}: add gpid({}) to dir_node({})", dsn_primary_address(), pid, dn->tag);
}

dir_node *fs_manager::find_best_dir_for_new_replica(const gpid &pid) const
{
    dir_node *selected = nullptr;
    uint64_t least_app_replicas_count = 0;
    uint64_t least_total_replicas_count = 0;
    {
        zauto_write_lock l(_lock);
        // Try to find the dir_node with the least replica count.
        for (const auto &dn : _dir_nodes) {
            CHECK(!dn->has(pid), "gpid({}) already exists in dir_node({})", pid, dn->tag);
            uint64_t app_replicas_count = dn->replicas_count(pid.get_app_id());
            uint64_t total_replicas_count = dn->replicas_count();

            if (selected == nullptr || least_app_replicas_count > app_replicas_count) {
                least_app_replicas_count = app_replicas_count;
                least_total_replicas_count = total_replicas_count;
                selected = dn.get();
            } else if (least_app_replicas_count == app_replicas_count &&
                       least_total_replicas_count > total_replicas_count) {
                least_total_replicas_count = total_replicas_count;
                selected = dn.get();
            }
        }
    }
    if (selected != nullptr) {
        LOG_INFO(
            "{}: put pid({}) to dir({}), which has {} replicas of current app, {} replicas totally",
            dsn_primary_address(),
            pid,
            selected->tag,
            least_app_replicas_count,
            least_total_replicas_count);
    }
    return selected;
}

void fs_manager::specify_dir_for_new_replica_for_test(dir_node *specified_dn,
                                                      const dsn::gpid &pid) const
{
    bool dn_found = false;
    zauto_write_lock l(_lock);
    for (const auto &dn : _dir_nodes) {
        CHECK(!dn->has(pid), "gpid({}) already exists in dir_node({})", pid, dn->tag);
        if (dn.get() == specified_dn) {
            dn_found = true;
        }
    }
    CHECK(dn_found, "dir_node({}) is not exist", specified_dn->tag);
    const auto dir = specified_dn->replica_dir("replica", pid);
    CHECK_TRUE(dsn::utils::filesystem::create_directory(dir));
    specified_dn->holding_replicas[pid.get_app_id()].emplace(pid);
}

void fs_manager::remove_replica(const gpid &pid)
{
    zauto_write_lock l(_lock);
    uint64_t remove_count = 0;
    for (auto &dn : _dir_nodes) {
        uint64_t r = dn->remove(pid);
        CHECK_LE_MSG(remove_count + r,
                     1,
                     "gpid({}) found in dir({}), which was removed before",
                     pid,
                     dn->tag);
        if (r != 0) {
            LOG_INFO("{}: remove gpid({}) from dir({})", dsn_primary_address(), pid, dn->tag);
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

void fs_manager::update_disk_stat()
{
    zauto_write_lock l(_lock);
    reset_disk_stat();
    for (auto &dn : _dir_nodes) {
        dn->update_disk_stat();
        _total_capacity_mb += dn->disk_capacity_mb;
        _total_available_mb += dn->disk_available_mb;
        _min_available_ratio = std::min(dn->disk_available_ratio, _min_available_ratio);
        _max_available_ratio = std::max(dn->disk_available_ratio, _max_available_ratio);
    }
    _total_available_ratio = static_cast<int>(
        _total_capacity_mb == 0 ? 0 : std::round(_total_available_mb * 100.0 / _total_capacity_mb));

    LOG_INFO("update disk space succeed: disk_count = {}, total_capacity_mb = {}, "
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
    LOG_INFO("{}: mark data dir({}) as tag({})", dsn_primary_address().to_string(), norm_path, tag);
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

dir_node *fs_manager::find_replica_dir(dsn::string_view app_type, gpid pid)
{
    std::string replica_dir;
    dir_node *replica_dn = nullptr;
    {
        zauto_read_lock l(_lock);
        for (const auto &dn : _dir_nodes) {
            const auto dir = dn->replica_dir(app_type, pid);
            if (utils::filesystem::directory_exists(dir)) {
                // Check if there are duplicate replica instance directories.
                CHECK(replica_dir.empty(), "replica dir conflict: {} <--> {}", dir, replica_dir);
                replica_dir = dir;
                replica_dn = dn.get();
            }
        }
    }

    return replica_dn;
}

dir_node *fs_manager::create_replica_dir_if_necessary(dsn::string_view app_type, gpid pid)
{
    // Try to find the replica directory.
    auto replica_dn = find_replica_dir(app_type, pid);
    if (replica_dn != nullptr) {
        CHECK_EQ_MSG(1, replica_dn->holding_replicas[pid.get_app_id()].count(pid), "{}", pid);
        return replica_dn;
    }

    // TODO(yingchun): enable this check after unit tests are refactored and fixed.
    // CHECK(0 == replica_dn->holding_replicas.count(pid.get_app_id()) ||
    //       0 == replica_dn->holding_replicas[pid.get_app_id()].count(pid), "");
    // Find a dir_node for the new replica.
    replica_dn = find_best_dir_for_new_replica(pid);
    if (replica_dn == nullptr) {
        return nullptr;
    }

    const auto dir = replica_dn->replica_dir(app_type, pid);
    if (!dsn::utils::filesystem::create_directory(dir)) {
        LOG_ERROR("create replica directory({}) failed", dir);
        return nullptr;
    }

    replica_dn->holding_replicas[pid.get_app_id()].emplace(pid);
    return replica_dn;
}

dir_node *fs_manager::create_child_replica_dir(dsn::string_view app_type,
                                               gpid child_pid,
                                               const std::string &parent_dir)
{
    dir_node *child_dn = nullptr;
    std::string child_dir;
    {
        zauto_read_lock l(_lock);
        for (const auto &dn : _dir_nodes) {
            child_dir = dn->replica_dir(app_type, child_pid);
            // <parent_dir> = <prefix>/<gpid>.<app_type>
            // check if <parent_dir>'s <prefix> is equal to <data_dir>
            // TODO(yingchun): use a function instead.
            if (parent_dir.substr(0, dn->full_dir.size() + 1) == dn->full_dir + "/") {
                child_dn = dn.get();
                break;
            }
        }
    }
    CHECK_NOTNULL(child_dn, "can not find parent_dir {} in data_dirs", parent_dir);
    if (!dsn::utils::filesystem::create_directory(child_dir)) {
        LOG_ERROR("create child replica directory({}) failed", child_dir);
        return nullptr;
    }
    add_replica(child_pid, child_dir);
    return child_dn;
}

} // namespace replication
} // namespace dsn
