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

#include "common/replication_common.h"

#include <string.h>
#include <fstream>
#include <memory>

#include "common/gpid.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "fmt/core.h"
#include "rpc/dns_resolver.h" // IWYU pragma: keep
#include "rpc/rpc_address.h"
#include "runtime/service_app.h"
#include "utils/config_api.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"
#include "utils/utils.h"

DSN_DEFINE_bool(replication, duplication_enabled, true, "is duplication enabled");

DSN_DEFINE_int32(replication,
                 mutation_2pc_min_replica_count,
                 2,
                 "The minimum number of ALIVE replicas under which write is allowed. It's valid if "
                 "larger than 0, otherwise, the final value is based on 'app_max_replica_count'");
DSN_DEFINE_int32(replication,
                 gc_interval_ms,
                 30 * 1000,
                 "The interval milliseconds to do replica statistics. The name contains 'gc' is "
                 "for legacy reason");
DSN_DEFINE_int32(replication,
                 fd_check_interval_seconds,
                 2,
                 "The interval seconds of failure detector to check healthiness of remote peers");
DSN_DEFINE_int32(replication,
                 fd_beacon_interval_seconds,
                 3,
                 "The interval seconds of failure detector to send beacon message to remote peers");
DSN_DEFINE_int32(replication,
                 fd_lease_seconds,
                 20,
                 "The lease in seconds get from remote FD master");
DSN_DEFINE_int32(replication,
                 fd_grace_seconds,
                 22,
                 "The grace in seconds assigned to remote FD slaves");
DSN_DEFINE_int32(replication,
                 cold_backup_checkpoint_reserve_minutes,
                 10,
                 "reserve minutes of cold backup checkpoint");

/**
 * Empty write is used for flushing WAL log entry which is submit asynchronously.
 * Make sure it can work well if you diable it.
 */
DSN_DEFINE_bool(replication,
                empty_write_disabled,
                false,
                "Whether to disable the function of primary replicas periodically "
                "generating empty write operations to check the group status");
DSN_TAG_VARIABLE(empty_write_disabled, FT_MUTABLE);

DSN_DEFINE_string(replication,
                  slog_dir,
                  "",
                  "The shared log directory. Deprecated since Pegasus 2.6.0, but "
                  "leave it and do not modify the value if upgrading from older versions.");
DSN_DEFINE_string(replication,
                  data_dirs,
                  "",
                  "A list of directories for replica data storage, it is recommended to "
                  "configure one item per disk. 'tag' is the tag name of the directory");
DSN_DEFINE_string(replication,
                  data_dirs_black_list_file,
                  "/home/work/.pegasus_data_dirs_black_list",
                  "Blacklist file, where each line is a path that needs to be ignored, mainly used "
                  "to filter out bad drives");
DSN_DEFINE_string(replication,
                  cold_backup_root,
                  "",
                  "The prefix of cold backup data path on remote storage");
DSN_DEFINE_string(meta_server,
                  server_list,
                  "",
                  "Comma-separated list of MetaServers in the Pegasus cluster");

namespace dsn {
namespace replication {
const std::string replication_options::kRepsDir = "reps";
const std::string replication_options::kReplicaAppType = "replica";

replication_options::~replication_options() {}

void replication_options::initialize()
{
    const service_app_info &info = service_app::current_service_app_info();
    app_name = info.full_name;
    app_dir = info.data_dir;

    if (strlen(FLAGS_slog_dir) == 0) {
        slog_dir = app_dir;
    } else {
        slog_dir = utils::filesystem::path_combine(slog_dir, app_name);
    }
    slog_dir = utils::filesystem::path_combine(slog_dir, "slog");

    // get config_data_dirs and config_data_dir_tags from config
    std::vector<std::string> config_data_dirs;
    std::vector<std::string> config_data_dir_tags;
    std::string error_msg = "";
    bool flag = get_data_dir_and_tag(
        FLAGS_data_dirs, app_dir, app_name, config_data_dirs, config_data_dir_tags, error_msg);
    CHECK(flag, error_msg);

    // check if data_dir in black list, data_dirs doesn't contain dir in black list
    std::vector<std::string> black_list_dirs;
    get_data_dirs_in_black_list(FLAGS_data_dirs_black_list_file, black_list_dirs);
    for (auto i = 0; i < config_data_dirs.size(); ++i) {
        if (check_if_in_black_list(black_list_dirs, config_data_dirs[i])) {
            continue;
        }
        data_dirs.emplace_back(config_data_dirs[i]);
        data_dir_tags.emplace_back(config_data_dir_tags[i]);
    }

    CHECK(!data_dirs.empty(), "no replica data dir found, maybe not set or excluded by black list");
    CHECK(replica_helper::parse_server_list(FLAGS_server_list, meta_servers),
          "invalid meta server config");
}

int32_t replication_options::app_mutation_2pc_min_replica_count(int32_t app_max_replica_count) const
{
    CHECK_GT(app_max_replica_count, 0);
    if (FLAGS_mutation_2pc_min_replica_count > 0) { //  >0 means use the user config
        return FLAGS_mutation_2pc_min_replica_count;
    } else { // otherwise, the value based on the table max_replica_count
        return app_max_replica_count <= 2 ? 1 : app_max_replica_count / 2 + 1;
    }
}

/*static*/ bool replica_helper::get_replica_config(const partition_configuration &pc,
                                                   const ::dsn::host_port &node,
                                                   /*out*/ replica_configuration &rc)
{
    rc.pid = pc.pid;
    rc.ballot = pc.ballot;
    rc.learner_signature = invalid_signature;
    SET_OBJ_IP_AND_HOST_PORT(rc, primary, pc, primary);

    if (node == pc.hp_primary) {
        rc.status = partition_status::PS_PRIMARY;
        return true;
    }

    if (utils::contains(pc.hp_secondaries, node)) {
        rc.status = partition_status::PS_SECONDARY;
        return true;
    }

    rc.status = partition_status::PS_INACTIVE;
    return false;
}

bool replica_helper::load_servers_from_config(const std::string &section,
                                              const std::string &key,
                                              /*out*/ std::vector<dsn::host_port> &servers)
{
    const auto *server_list = dsn_config_get_value_string(section.c_str(), key.c_str(), "", "");
    return dsn::replication::replica_helper::parse_server_list(server_list, servers);
}

bool replica_helper::parse_server_list(const char *server_list,
                                       /*out*/ std::vector<dsn::host_port> &servers)
{
    servers.clear();
    std::vector<std::string> host_port_strs;
    ::dsn::utils::split_args(server_list, host_port_strs, ',');
    for (const auto &host_port_str : host_port_strs) {
        const auto hp = dsn::host_port::from_string(host_port_str);
        if (!hp) {
            LOG_ERROR("invalid host_port '{}' specified in '{}'", host_port_str, server_list);
            return false;
        }
        servers.push_back(hp);
    }

    if (servers.empty()) {
        LOG_ERROR("no meta server specified");
        return false;
    }
    if (servers.size() != host_port_strs.size()) {
        LOG_ERROR("server_list {} have duplicate server", server_list);
        return false;
    }
    return true;
}

/*static*/ bool
replication_options::get_data_dir_and_tag(const std::string &config_dirs_str,
                                          const std::string &default_dir,
                                          const std::string &app_name,
                                          /*out*/ std::vector<std::string> &data_dirs,
                                          /*out*/ std::vector<std::string> &data_dir_tags,
                                          /*out*/ std::string &err_msg)
{
    // - if {config_dirs_str} is empty (return true):
    //   - dir = {default_dir}
    //   - dir_tag/data_dir_tag = "default"
    //   - data_dir = {default_dir}/"reps"
    // - else if {config_dirs_str} = "tag1:dir1,tag2:dir2:tag3:dir3" (return true):
    //   - dir1 = "dir1"/{app_name}
    //   - dir_tag1/data_dir_tag1 = "tag1"
    //   - data_dir1 = "dir1"/{app_name}/"reps"
    // - else (return false):
    //   - invalid format and set {err_msg}
    std::vector<std::string> dirs;
    std::vector<std::string> dir_tags;
    utils::split_args(config_dirs_str.c_str(), dirs, ',');
    if (dirs.empty()) {
        dirs.push_back(default_dir);
        dir_tags.push_back("default");
    } else {
        for (auto &dir : dirs) {
            std::vector<std::string> tag_and_dir;
            utils::split_args(dir.c_str(), tag_and_dir, ':');
            if (tag_and_dir.size() != 2) {
                err_msg = fmt::format("invalid data_dir item({}) in config", dir);
                return false;
            }
            if (tag_and_dir[0].empty() || tag_and_dir[1].empty()) {
                err_msg = fmt::format("invalid data_dir item({}) in config", dir);
                return false;
            }
            dir = utils::filesystem::path_combine(tag_and_dir[1], app_name);
            for (unsigned i = 0; i < dir_tags.size(); ++i) {
                if (dirs[i] == dir) {
                    err_msg = fmt::format("dir({}) and dir({}) conflict", dirs[i], dir);
                    return false;
                }
            }
            for (unsigned i = 0; i < dir_tags.size(); ++i) {
                if (dir_tags[i] == tag_and_dir[0]) {
                    err_msg = fmt::format(
                        "dir({}) and dir({}) have same tag({})", dirs[i], dir, tag_and_dir[0]);
                    return false;
                }
            }
            dir_tags.push_back(tag_and_dir[0]);
        }
    }

    for (unsigned i = 0; i < dirs.size(); ++i) {
        const std::string &dir = dirs[i];
        LOG_INFO("data_dirs[{}] = {}, tag = {}", i + 1, dir, dir_tags[i]);
        data_dirs.push_back(utils::filesystem::path_combine(dir, kRepsDir));
        data_dir_tags.push_back(dir_tags[i]);
    }
    return true;
}

/*static*/ void
replication_options::get_data_dirs_in_black_list(const std::string &fname,
                                                 /*out*/ std::vector<std::string> &dirs)
{
    if (fname.empty() || !utils::filesystem::file_exists(fname)) {
        LOG_INFO("data_dirs_black_list_file[{}] not found, ignore it", fname);
        return;
    }

    LOG_INFO("data_dirs_black_list_file[{}] found, apply it", fname);
    std::ifstream file(fname);
    CHECK(file, "open data_dirs_black_list_file failed: {}", fname);

    std::string str;
    int count = 0;
    while (std::getline(file, str)) {
        std::string str2 = utils::trim_string(const_cast<char *>(str.c_str()));
        if (str2.empty()) {
            continue;
        }
        if (str2.back() != '/') {
            str2.append("/");
        }
        dirs.push_back(str2);
        count++;
        LOG_INFO("black_list[{}] = [{}]", count, str2);
    }
}

/*static*/ bool
replication_options::check_if_in_black_list(const std::vector<std::string> &black_list_dir,
                                            const std::string &dir)
{
    std::string dir_str = dir;
    if (!black_list_dir.empty()) {
        if (dir_str.back() != '/') {
            dir_str.append("/");
        }
        for (const std::string &black : black_list_dir) {
            if (dir_str.find(black) == 0) {
                return true;
            }
        }
    }
    return false;
}

} // namespace replication
} // namespace dsn
