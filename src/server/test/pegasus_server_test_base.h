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

#pragma once

#include "server/pegasus_server_impl.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "common/fs_manager.h"
#include "utils/flags.h"
#include "replica/replica_stub.h"
#include "test_util/test_util.h"
#include "utils/filesystem.h"

DSN_DECLARE_bool(encrypt_data_at_rest);

namespace pegasus {
namespace server {

class mock_pegasus_server_impl : public pegasus_server_impl
{
public:
    mock_pegasus_server_impl(dsn::replication::replica *r) : pegasus_server_impl(r) {}

public:
    MOCK_CONST_METHOD0(is_duplication_follower, bool());
};

class pegasus_server_test_base : public pegasus::encrypt_data_test_base
{
public:
    pegasus_server_test_base()
    {
        // Remove rdb to prevent rocksdb recovery from last test.
        dsn::utils::filesystem::remove_path("./test_dir");
        _replica_stub = new dsn::replication::replica_stub();
        _replica_stub->get_fs_manager()->initialize({"test_dir"}, {"test_tag"});

        // Use different gpid for encryption and non-encryption test to avoid reopening a rocksdb
        // instance with different encryption option.
        if (FLAGS_encrypt_data_at_rest) {
            _gpid = dsn::gpid(100, 0);
        } else {
            _gpid = dsn::gpid(100, 1);
        }

        dsn::app_info app_info;
        app_info.app_type = "pegasus";

        auto *dn = _replica_stub->get_fs_manager()->find_best_dir_for_new_replica(_gpid);
        CHECK_NOTNULL(dn, "");
        _replica = new dsn::replication::replica(_replica_stub, _gpid, app_info, dn, false, false);
        const auto dir_data = dsn::utils::filesystem::path_combine(_replica->dir(), "data");
        CHECK(dsn::utils::filesystem::create_directory(dir_data),
              "create data dir {} failed",
              dir_data);

        _server = std::make_unique<mock_pegasus_server_impl>(_replica);
    }

    dsn::error_code start(const std::map<std::string, std::string> &envs = {})
    {
        std::unique_ptr<char *[]> argvs = std::make_unique<char *[]>(1 + envs.size() * 2);
        char **argv = argvs.get();
        int idx = 0;
        argv[idx++] = const_cast<char *>("unit_test_app");
        if (!envs.empty()) {
            for (auto &kv : envs) {
                argv[idx++] = const_cast<char *>(kv.first.c_str());
                argv[idx++] = const_cast<char *>(kv.second.c_str());
            }
        }
        return _server->start(idx, argv);
    }

    ~pegasus_server_test_base() override
    {
        // do not clear state
        _server->stop(false);

        delete _replica_stub;
        delete _replica;
    }

protected:
    std::unique_ptr<mock_pegasus_server_impl> _server;
    dsn::replication::replica *_replica = nullptr;
    dsn::replication::replica_stub *_replica_stub = nullptr;
    dsn::gpid _gpid;
};

} // namespace server
} // namespace pegasus
