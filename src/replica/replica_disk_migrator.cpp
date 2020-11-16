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

#include "replica/replica_stub.h"
#include "replica_disk_migrator.h"

#include <dsn/dist/fmt_logging.h>

namespace dsn {
namespace replication {

replica_disk_migrator::replica_disk_migrator(replica *r) : replica_base(r), _replica(r) {}

replica_disk_migrator::~replica_disk_migrator() = default;

// THREAD_POOL_REPLICATION
void replica_disk_migrator::on_migrate_replica(const replica_disk_migrate_request &req,
                                               /*out*/ replica_disk_migrate_response &resp)
{
    if (!check_migration_args(req, resp)) {
        return;
    }

    _status = disk_migration_status::MOVING;
    ddebug_replica(
        "received replica disk migrate request(origin={}, target={}), update status from {}=>{}",
        req.origin_disk,
        req.target_disk,
        enum_to_string(disk_migration_status::IDLE),
        enum_to_string(status()));

    tasking::enqueue(
        LPC_REPLICATION_LONG_COMMON, _replica->tracker(), [=]() { migrate_replica(req); });
}

bool replica_disk_migrator::check_migration_args(const replica_disk_migrate_request &req,
                                                 /*out*/ replica_disk_migrate_response &resp)
{
    _replica->_checker.only_one_thread_access();

    // TODO(jiashuo1) may need manager control migration flow
    if (status() != disk_migration_status::IDLE) {
        std::string err_msg =
            fmt::format("Existed migrate task({}) is running", enum_to_string(status()));
        derror_replica("received replica disk migrate request(origin={}, target={}), err = {}",
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
        derror_replica("received replica disk migrate request(origin={}, target={}), err = {}",
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
        derror_replica("received replica disk migrate request(origin={}, target={}), err = {}",
                       req.origin_disk,
                       req.target_disk,
                       err_msg);
        resp.err = ERR_INVALID_PARAMETERS;
        resp.__set_hint(err_msg);
        return false;
    }

    bool valid_origin_disk = false;
    bool valid_target_disk = false;
    // _dir_nodes: std::vector<std::shared_ptr<dir_node>>
    for (const auto &dir_node : _replica->get_replica_stub()->_fs_manager._dir_nodes) {
        if (dir_node->tag == req.origin_disk) {
            valid_origin_disk = true;
            if (!dir_node->has(req.pid)) {
                std::string err_msg =
                    fmt::format("Invalid replica(replica({}) doesn't exist on origin disk({}))",
                                req.pid,
                                req.origin_disk);
                derror_replica(
                    "received replica disk migrate request(origin={}, target={}), err = {}",
                    req.origin_disk,
                    req.target_disk,
                    err_msg);
                resp.err = ERR_OBJECT_NOT_FOUND;
                resp.__set_hint(err_msg);
                return false;
            }
        }

        if (dir_node->tag == req.target_disk) {
            valid_target_disk = true;
            if (dir_node->has(get_gpid())) {
                std::string err_msg =
                    fmt::format("Invalid replica(replica({}) has existed on target disk({}))",
                                req.pid,
                                req.target_disk);
                derror_replica(
                    "received replica disk migrate request(origin={}, target={}), err = {}",
                    req.origin_disk,
                    req.target_disk,
                    err_msg);
                resp.err = ERR_PATH_ALREADY_EXIST;
                resp.__set_hint(err_msg);
                return false;
            }
        }
    }

    if (!valid_origin_disk || !valid_target_disk) {
        std::string invalid_disk_tag = !valid_origin_disk ? req.origin_disk : req.target_disk;
        std::string err_msg = fmt::format("Invalid disk tag({} doesn't exist)", invalid_disk_tag);
        derror_replica("received replica disk migrate request(origin={}, target={}), err = {}",
                       req.origin_disk,
                       req.target_disk,
                       err_msg);
        resp.err = ERR_OBJECT_NOT_FOUND;
        resp.__set_hint(err_msg);
        return false;
    }

    resp.err = ERR_OK;
    return true;
}

// TODO(jiashuo1)
// THREAD_POOL_REPLICATION_LONG
void replica_disk_migrator::migrate_replica(const replica_disk_migrate_request &req) {}

// TODO(jiashuo1)
// THREAD_POOL_REPLICATION_LONG
void replica_disk_migrator::update_replica_dir() {}
} // namespace replication
} // namespace dsn
