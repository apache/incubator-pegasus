/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <boost/algorithm/string/replace.hpp>
#include <fmt/core.h>

#include <string_view>
#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "metadata_types.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "replica_disk_migrator.h"
#include "task/async_calls.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/thread_access_checker.h"
#include "utils/load_dump_object.h"

namespace dsn {
namespace replication {

const std::string replica_disk_migrator::kReplicaDirTempSuffix = ".disk.migrate.tmp";
const std::string replica_disk_migrator::kReplicaDirOriginSuffix = ".disk.migrate.ori";
const std::string replica_disk_migrator::kDataDirFolder = "data/rdb/";

replica_disk_migrator::replica_disk_migrator(replica *r) : replica_base(r), _replica(r) {}

replica_disk_migrator::~replica_disk_migrator() = default;

// THREAD_POOL_DEFAULT
void replica_disk_migrator::on_migrate_replica(replica_disk_migrate_rpc rpc)
{
    tasking::enqueue(
        LPC_REPLICATION_COMMON,
        _replica->tracker(),
        [=]() {
            if (!check_migration_args(rpc)) {
                return;
            }

            _status = disk_migration_status::MOVING;
            LOG_INFO_PREFIX(
                "received replica disk migrate request(origin={}, target={}), update status "
                "from {}=>{}",
                rpc.request().origin_disk,
                rpc.request().target_disk,
                enum_to_string(disk_migration_status::IDLE),
                enum_to_string(status()));

            const auto &request = rpc.request();
            tasking::enqueue(LPC_REPLICATION_LONG_COMMON, _replica->tracker(), [=]() {
                migrate_replica(request);
            });
        },
        get_gpid().thread_hash());
}

// THREAD_POOL_REPLICATION
bool replica_disk_migrator::check_migration_args(replica_disk_migrate_rpc rpc)
{
    _replica->_checker.only_one_thread_access();

    const replica_disk_migrate_request &req = rpc.request();
    replica_disk_migrate_response &resp = rpc.response();

    // TODO(jiashuo1) may need manager control migration flow
    if (status() != disk_migration_status::IDLE) {
        std::string err_msg =
            fmt::format("Existed migrate task({}) is running", enum_to_string(status()));
        LOG_ERROR_PREFIX("received replica disk migrate request(origin={}, target={}), err = {}",
                         req.origin_disk,
                         req.target_disk,
                         err_msg);
        resp.err = ERR_BUSY;
        resp.__set_hint(err_msg);
        return false;
    }

    if (_replica->status() != partition_status::type::PS_SECONDARY) {
        std::string err_msg =
            fmt::format("Invalid partition status({})", enum_to_string(_replica->status()));
        LOG_ERROR_PREFIX("received replica disk migrate request(origin={}, target={}), err = {}",
                         req.origin_disk,
                         req.target_disk,
                         err_msg);
        resp.err = ERR_INVALID_STATE;
        resp.__set_hint(err_msg);
        return false;
    }

    if (req.origin_disk == req.target_disk) {
        std::string err_msg = fmt::format(
            "Invalid disk tag(origin({}) equal target({}))", req.origin_disk, req.target_disk);
        LOG_ERROR_PREFIX("received replica disk migrate request(origin={}, target={}), err = {}",
                         req.origin_disk,
                         req.target_disk,
                         err_msg);
        resp.err = ERR_INVALID_PARAMETERS;
        resp.__set_hint(err_msg);
        return false;
    }

    std::string err_msg;
    auto ec = _replica->get_replica_stub()->_fs_manager.validate_migrate_op(
        req.pid, req.origin_disk, req.target_disk, err_msg);
    if (ec != ERR_OK) {
        LOG_ERROR_PREFIX(err_msg);
        resp.err = ec;
        resp.__set_hint(err_msg);
        return false;
    }

    resp.err = ERR_OK;
    return true;
}

// THREAD_POOL_REPLICATION_LONG
void replica_disk_migrator::migrate_replica(const replica_disk_migrate_request &req)
{
    CHECK_EQ_PREFIX_MSG(status(),
                        disk_migration_status::MOVING,
                        "disk migration(origin={}, target={}), err = Invalid migration status({})",
                        req.origin_disk,
                        req.target_disk,
                        enum_to_string(status()));

    if (init_target_dir(req) && migrate_replica_checkpoint(req) && migrate_replica_app_info(req)) {
        _status = disk_migration_status::MOVED;
        LOG_INFO_PREFIX("disk migration(origin={}, target={}) copy data complete, update status "
                        "from {}=>{}, ready to "
                        "close origin replica({})",
                        req.origin_disk,
                        req.target_disk,
                        enum_to_string(disk_migration_status::MOVING),
                        enum_to_string(status()),
                        _replica->dir());

        close_current_replica(req);
    }
}

// THREAD_POOL_REPLICATION_LONG
bool replica_disk_migrator::init_target_dir(const replica_disk_migrate_request &req)
{
    FAIL_POINT_INJECT_F("init_target_dir", [this](std::string_view) -> bool {
        reset_status();
        return false;
    });
    // replica_dir: /root/origin_disk_tag/gpid.app_type
    std::string replica_dir = _replica->dir();
    // using origin dir to init new dir
    boost::replace_first(replica_dir, req.origin_disk, req.target_disk);
    if (utils::filesystem::directory_exists(replica_dir)) {
        LOG_ERROR_PREFIX("migration target replica dir({}) has existed", replica_dir);
        reset_status();
        return false;
    }

    // _target_replica_dir = /root/target_disk_tag/gpid.app_type.disk.migrate.tmp, it will update to
    // /root/target_disk_tag/gpid.app_type in replica_disk_migrator::update_replica_dir finally
    _target_replica_dir = fmt::format("{}{}", replica_dir, kReplicaDirTempSuffix);
    if (utils::filesystem::directory_exists(_target_replica_dir)) {
        LOG_WARNING_PREFIX(
            "disk migration(origin={}, target={}) target replica dir({}) has existed, "
            "delete it now",
            req.origin_disk,
            req.target_disk,
            _target_replica_dir);
        utils::filesystem::remove_path(_target_replica_dir);
    }

    //  _target_replica_data_dir = /root/gpid.app_type.disk.migrate.tmp/data/rdb, it will update to
    //  /root/target/gpid.app_type/data/rdb in replica_disk_migrator::update_replica_dir finally
    _target_data_dir = utils::filesystem::path_combine(_target_replica_dir, kDataDirFolder);
    if (!utils::filesystem::create_directory(_target_data_dir)) {
        LOG_ERROR_PREFIX(
            "disk migration(origin={}, target={}) create target temp data dir({}) failed",
            req.origin_disk,
            req.target_disk,
            _target_data_dir);
        reset_status();
        return false;
    }

    return true;
}

// THREAD_POOL_REPLICATION_LONG
bool replica_disk_migrator::migrate_replica_checkpoint(const replica_disk_migrate_request &req)
{
    FAIL_POINT_INJECT_F("migrate_replica_checkpoint", [this](std::string_view) -> bool {
        reset_status();
        return false;
    });

    const auto &sync_checkpoint_err = _replica->get_app()->sync_checkpoint();
    if (sync_checkpoint_err != ERR_OK) {
        LOG_ERROR_PREFIX("disk migration(origin={}, target={}) sync_checkpoint failed({})",
                         req.origin_disk,
                         req.target_disk,
                         sync_checkpoint_err);
        reset_status();
        return false;
    }

    const auto &copy_checkpoint_err =
        _replica->get_app()->copy_checkpoint_to_dir(_target_data_dir.c_str(), 0 /*last_decree*/);
    if (copy_checkpoint_err != ERR_OK) {
        LOG_ERROR_PREFIX("disk migration(origin={}, target={}) copy checkpoint to dir({}) "
                         "failed(error={}), the dir({}) will be deleted",
                         req.origin_disk,
                         req.target_disk,
                         _target_data_dir,
                         copy_checkpoint_err,
                         _target_replica_dir);
        reset_status();
        utils::filesystem::remove_path(_target_replica_dir);
        return false;
    }

    return true;
}

// THREAD_POOL_REPLICATION_LONG
bool replica_disk_migrator::migrate_replica_app_info(const replica_disk_migrate_request &req)
{
    FAIL_POINT_INJECT_F("migrate_replica_app_info", [this](std::string_view) -> bool {
        reset_status();
        return false;
    });
    replica_init_info init_info = _replica->get_app()->init_info();
    const auto &store_init_info_err = utils::dump_rjobj_to_file(
        init_info,
        utils::filesystem::path_combine(_target_replica_dir, replica_init_info::kInitInfo));
    if (store_init_info_err != ERR_OK) {
        LOG_ERROR_PREFIX("disk migration(origin={}, target={}) stores app init info failed({})",
                         req.origin_disk,
                         req.target_disk,
                         store_init_info_err);
        reset_status();
        return false;
    }

    const auto &store_info_err = _replica->store_app_info(
        _replica->_app_info,
        utils::filesystem::path_combine(_target_replica_dir, replica_app_info::kAppInfo));
    if (store_info_err != ERR_OK) {
        LOG_ERROR_PREFIX("disk migration(origin={}, target={}) stores app info failed({})",
                         req.origin_disk,
                         req.target_disk,
                         store_info_err);
        reset_status();
        return false;
    }

    return true;
}

// THREAD_POOL_REPLICATION_LONG
dsn::task_ptr replica_disk_migrator::close_current_replica(const replica_disk_migrate_request &req)
{
    if (_replica->status() != partition_status::type::PS_SECONDARY) {
        LOG_ERROR_PREFIX(
            "migrate request(origin={}, target={}), err = Invalid partition status({})",
            req.origin_disk,
            req.target_disk,
            enum_to_string(_replica->status()));
        reset_status();
        utils::filesystem::remove_path(_target_replica_dir);
        return nullptr;
    }

    return _replica->_stub->begin_close_replica(_replica);
}

// run in replica->close_replica() of THREAD_POOL_REPLICATION_LONG
void replica_disk_migrator::update_replica_dir()
{
    // origin_tmp_dir: /root/origin/gpid.app_type.disk.migrate.ori
    std::string origin_temp_dir = fmt::format("{}{}", _replica->dir(), kReplicaDirOriginSuffix);
    if (!dsn::utils::filesystem::rename_path(_replica->dir(), origin_temp_dir)) {
        reset_status();
        utils::filesystem::remove_path(_target_replica_dir);
        return;
    }

    std::string target_temp_dir = _target_replica_dir;
    // update _target_replica_dir /root/gpid.app_type.disk.migrate.tmp/ to
    // /root/target/gpid.app_type/
    boost::replace_first(_target_replica_dir, kReplicaDirTempSuffix, "");
    if (!dsn::utils::filesystem::rename_path(target_temp_dir, _target_replica_dir)) {
        reset_status();
        // rename failed, delete tmp dir and revert origin dir
        utils::filesystem::remove_path(target_temp_dir);
        dsn::utils::filesystem::rename_path(origin_temp_dir, _replica->dir());
        return;
    }

    _replica->get_replica_stub()->_fs_manager.remove_replica(get_gpid());
    _replica->get_replica_stub()->_fs_manager.add_replica(get_gpid(), _target_replica_dir);
    _replica->get_replica_stub()->on_disk_stat();

    _status = disk_migration_status::CLOSED;
    LOG_INFO_PREFIX("disk replica migration move data from origin dir({}) to new dir({}) "
                    "succeed, update status from {}=>{}",
                    _replica->dir(),
                    _target_replica_dir,
                    enum_to_string(disk_migration_status::MOVED),
                    enum_to_string(status()));
}
} // namespace replication
} // namespace dsn
