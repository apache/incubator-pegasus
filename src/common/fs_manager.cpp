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

#include "fs_manager.h"

#include <fmt/std.h> // IWYU pragma: keep
#include <algorithm>
#include <cstdint>
#include <utility>

#include <string_view>
#include "common/gpid.h"
#include "common/replication_enums.h"
#include "fmt/core.h"
#include "replica_admin_types.h"
#include "runtime/api_layer1.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/math.h"
#include "utils/ports.h"

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

METRIC_DEFINE_entity(disk);

METRIC_DEFINE_gauge_int64(disk,
                          disk_capacity_total_mb,
                          dsn::metric_unit::kMegaBytes,
                          "The total disk capacity");

METRIC_DEFINE_gauge_int64(disk,
                          disk_capacity_avail_mb,
                          dsn::metric_unit::kMegaBytes,
                          "The available disk capacity");

namespace dsn {
namespace replication {

error_code disk_status_to_error_code(disk_status::type ds)
{
    switch (ds) {
    case disk_status::SPACE_INSUFFICIENT:
        return dsn::ERR_DISK_INSUFFICIENT;
    case disk_status::IO_ERROR:
        return dsn::ERR_DISK_IO_ERROR;
    default:
        CHECK_EQ(disk_status::NORMAL, ds);
        return dsn::ERR_OK;
    }
}

namespace {

metric_entity_ptr instantiate_disk_metric_entity(const std::string &tag,
                                                 const std::string &data_dir)
{
    auto entity_id = fmt::format("disk@{}", tag);

    return METRIC_ENTITY_disk.instantiate(entity_id, {{"tag", tag}, {"data_dir", data_dir}});
}

} // anonymous namespace

disk_capacity_metrics::disk_capacity_metrics(const std::string &tag, const std::string &data_dir)
    : _tag(tag),
      _data_dir(data_dir),
      _disk_metric_entity(instantiate_disk_metric_entity(tag, data_dir)),
      METRIC_VAR_INIT_disk(disk_capacity_total_mb),
      METRIC_VAR_INIT_disk(disk_capacity_avail_mb)
{
}

const metric_entity_ptr &disk_capacity_metrics::disk_metric_entity() const
{
    CHECK_NOTNULL(_disk_metric_entity,
                  "disk metric entity (tag={}, data_dir={}) should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate metric",
                  _tag,
                  _data_dir);
    return _disk_metric_entity;
}

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

std::string dir_node::replica_dir(std::string_view app_type, const dsn::gpid &pid) const
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
    FAIL_POINT_INJECT_F("update_disk_stat", [](std::string_view) { return; });

    dsn::utils::filesystem::disk_space_info dsi;
    if (!dsn::utils::filesystem::get_disk_space_info(full_dir, dsi)) {
        // TODO(yingchun): it may encounter some IO errors when get_disk_space_info() failed, deal
        //  with it.
        LOG_ERROR("get disk space info failed, dir = {}", full_dir);
        return;
    }

    disk_capacity_mb = dsi.capacity >> 20;
    disk_available_mb = dsi.available >> 20;
    disk_available_ratio = dsn::utils::calc_percentage<int>(disk_available_mb, disk_capacity_mb);

    METRIC_SET(disk_capacity, disk_capacity_total_mb, disk_capacity_mb);
    METRIC_SET(disk_capacity, disk_capacity_avail_mb, disk_available_mb);

    // It's able to change status from NORMAL to SPACE_INSUFFICIENT, and vice versa.
    const disk_status::type old_status = status;
    const auto new_status = disk_available_ratio < FLAGS_disk_min_available_space_ratio
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
        disk_status::type status = disk_status::NORMAL;
        if (dsn_unlikely(!utils::filesystem::create_directory(dir, cdir, err_msg) ||
                         !utils::filesystem::check_dir_rw(dir, err_msg))) {
            if (FLAGS_ignore_broken_disk) {
                LOG_ERROR("data dir({}) is broken, ignore it, error: {}", dir, err_msg);
            } else {
                CHECK(false, err_msg);
            }
            status = disk_status::IO_ERROR;
        }

        // Normalize the data directories.
        std::string norm_path;
        utils::filesystem::get_normalized_path(cdir, norm_path);

        // Create and add this dir_node.
        auto dn = std::make_shared<dir_node>(dir_tag, norm_path, 0, 0, 0, status);
        dir_nodes.emplace_back(dn);
        LOG_INFO("mark data dir({}) as tag({}) with status({})",
                 norm_path,
                 dir_tag,
                 enum_to_string(status));
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
            "{}: dir({}) of gpid({}) haven't registered", dsn_primary_host_port(), pid_dir, pid);
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
            "{}: gpid({}) already in the dir_node({})", dsn_primary_host_port(), pid, dn->tag);
        return;
    }

    LOG_INFO("{}: add gpid({}) to dir_node({})", dsn_primary_host_port(), pid, dn->tag);
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
            // Do not allocate new replica on dir_node which is not NORMAL.
            if (dn->status != disk_status::NORMAL) {
                LOG_INFO("skip the {} state dir_node({})", enum_to_string(dn->status), dn->tag);
                continue;
            }
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
            dsn_primary_host_port(),
            pid,
            selected->tag,
            least_app_replicas_count,
            least_total_replicas_count);
    }
    return selected;
}

void fs_manager::specify_dir_for_new_replica_for_test(dir_node *specified_dn,
                                                      std::string_view app_type,
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
    CHECK_EQ(disk_status::NORMAL, specified_dn->status);
    const auto dir = specified_dn->replica_dir(app_type, pid);
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
            LOG_INFO("{}: remove gpid({}) from dir({})", dsn_primary_host_port(), pid, dn->tag);
        }
        remove_count += r;
    }
}

void fs_manager::update_disk_stat()
{
    int64_t total_capacity_mb = 0;
    int64_t total_available_mb = 0;
    int total_available_ratio = 0;
    int min_available_ratio = 100;
    int max_available_ratio = 0;

    zauto_write_lock l(_lock);

    for (auto &dn : _dir_nodes) {
        // If the disk is already in IO_ERROR status, it will not change to other status, just skip
        // it.
        if (dn->status == disk_status::IO_ERROR) {
            LOG_WARNING("skip to update disk stat for dir({}), because it is in IO_ERROR status",
                        dn->tag);
            continue;
        }
        dn->update_disk_stat();
        total_capacity_mb += dn->disk_capacity_mb;
        total_available_mb += dn->disk_available_mb;
        min_available_ratio = std::min(dn->disk_available_ratio, min_available_ratio);
        max_available_ratio = std::max(dn->disk_available_ratio, max_available_ratio);
    }
    total_available_ratio = dsn::utils::calc_percentage<int>(total_available_mb, total_capacity_mb);

    LOG_INFO("update disk space succeed: disk_count = {}, total_capacity_mb = {}, "
             "total_available_mb = {}, total_available_ratio = {}%, min_available_ratio = {}%, "
             "max_available_ratio = {}%",
             _dir_nodes.size(),
             total_capacity_mb,
             total_available_mb,
             total_available_ratio,
             min_available_ratio,
             max_available_ratio);

    _total_capacity_mb.store(total_capacity_mb, std::memory_order_relaxed);
    _total_available_mb.store(total_available_mb, std::memory_order_relaxed);
}

void fs_manager::add_new_dir_node(const std::string &data_dir, const std::string &tag)
{
    std::string norm_path;
    utils::filesystem::get_normalized_path(data_dir, norm_path);
    auto dn = std::make_shared<dir_node>(tag, norm_path);

    {
        zauto_write_lock l(_lock);
        _dir_nodes.emplace_back(dn);
    }
    LOG_INFO("add new data dir({}) and mark as tag({})", norm_path, tag);
}

bool fs_manager::is_dir_node_exist(const std::string &data_dir, const std::string &tag) const
{
    std::string norm_path;
    utils::filesystem::get_normalized_path(data_dir, norm_path);

    zauto_read_lock l(_lock);
    for (const auto &dn : _dir_nodes) {
        if (dn->full_dir == norm_path || dn->tag == tag) {
            return true;
        }
    }
    return false;
}

dir_node *fs_manager::find_replica_dir(std::string_view app_type, gpid pid)
{
    std::string replica_dir;
    dir_node *replica_dn = nullptr;
    {
        zauto_read_lock l(_lock);
        for (const auto &dn : _dir_nodes) {
            // Skip IO error dir_node.
            if (dn->status == disk_status::IO_ERROR) {
                LOG_INFO("skip the {} state dir_node({})", enum_to_string(dn->status), dn->tag);
                continue;
            }
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

dir_node *fs_manager::create_replica_dir_if_necessary(std::string_view app_type, gpid pid)
{
    // Try to find the replica directory.
    auto replica_dn = find_replica_dir(app_type, pid);
    if (replica_dn != nullptr) {
        // TODO(yingchun): enable this check after unit tests are refactored and fixed.
        // CHECK(replica_dn->has(pid),
        //       "replica({})'s directory({}) exists but not in management",
        //       pid,
        //       replica_dn->tag);
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

dir_node *fs_manager::create_child_replica_dir(std::string_view app_type,
                                               gpid child_pid,
                                               const std::string &parent_dir)
{
    dir_node *child_dn = nullptr;
    std::string child_dir;
    {
        zauto_read_lock l(_lock);
        for (const auto &dn : _dir_nodes) {
            // Skip non-available dir_node.
            if (dn->status != disk_status::NORMAL) {
                LOG_INFO("skip the {} state dir_node({})", enum_to_string(dn->status), dn->tag);
                continue;
            }
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

std::vector<disk_info> fs_manager::get_disk_infos(int app_id) const
{
    std::vector<disk_info> disk_infos;
    zauto_read_lock l(_lock);
    for (const auto &dn : _dir_nodes) {
        disk_info di;
        // Query all app info if 'app_id' is 0, which is not a valid app id.
        if (app_id == 0) {
            di.holding_primary_replicas = dn->holding_primary_replicas;
            di.holding_secondary_replicas = dn->holding_secondary_replicas;
        } else {
            const auto &primary_replicas = dn->holding_primary_replicas.find(app_id);
            if (primary_replicas != dn->holding_primary_replicas.end()) {
                di.holding_primary_replicas[app_id] = primary_replicas->second;
            }

            const auto &secondary_replicas = dn->holding_secondary_replicas.find(app_id);
            if (secondary_replicas != dn->holding_secondary_replicas.end()) {
                di.holding_secondary_replicas[app_id] = secondary_replicas->second;
            }
        }
        di.tag = dn->tag;
        di.full_dir = dn->full_dir;
        di.disk_capacity_mb = dn->disk_capacity_mb;
        di.disk_available_mb = dn->disk_available_mb;

        disk_infos.emplace_back(std::move(di));
    }

    return disk_infos;
}

error_code fs_manager::validate_migrate_op(gpid pid,
                                           const std::string &origin_disk,
                                           const std::string &target_disk,
                                           std::string &err_msg) const
{
    bool origin_disk_exist = false;
    bool target_disk_exist = false;
    zauto_read_lock l(_lock);
    for (const auto &dn : _dir_nodes) {
        // Check if the origin directory is valid.
        if (dn->tag == origin_disk) {
            CHECK_FALSE(origin_disk_exist);
            if (!dn->has(pid)) {
                err_msg = fmt::format(
                    "replica({}) doesn't exist on the origin disk({})", pid, origin_disk);
                return ERR_OBJECT_NOT_FOUND;
            }

            // It's OK to migrate a replica from a dir_node which is NORMAL or even
            // SPACE_INSUFFICIENT, but not allowed when it's IO_ERROR.
            if (dn->status == disk_status::IO_ERROR) {
                err_msg = fmt::format(
                    "replica({}) exists on an IO-Error origin disk({})", pid, origin_disk);
                return ERR_DISK_IO_ERROR;
            }

            origin_disk_exist = true;
        }

        // Check if the target directory is valid.
        if (dn->tag == target_disk) {
            CHECK_FALSE(target_disk_exist);
            if (dn->has(pid)) {
                err_msg =
                    fmt::format("replica({}) already exists on target disk({})", pid, target_disk);
                return ERR_PATH_ALREADY_EXIST;
            }

            // It's not allowed to migrate a replica to a dir_node which is either
            // SPACE_INSUFFICIENT or IO_ERROR.
            if (dn->status == disk_status::SPACE_INSUFFICIENT ||
                dn->status == disk_status::IO_ERROR) {
                err_msg = fmt::format("replica({}) target disk({}) is {}",
                                      pid,
                                      origin_disk,
                                      enum_to_string(dn->status));
                return disk_status_to_error_code(dn->status);
            }

            target_disk_exist = true;
        }
    }

    if (!origin_disk_exist) {
        err_msg = fmt::format("origin disk({}) doesn't exist", origin_disk);
        return ERR_OBJECT_NOT_FOUND;
    }

    if (!target_disk_exist) {
        err_msg = fmt::format("target disk({}) doesn't exist", target_disk);
        return ERR_OBJECT_NOT_FOUND;
    }

    return ERR_OK;
}

} // namespace replication
} // namespace dsn
