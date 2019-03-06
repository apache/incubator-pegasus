// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "server/pegasus_server_impl.h"

#include <gtest/gtest.h>
#include <dsn/dist/replication/replica_test_utils.h>
#include <dsn/utility/filesystem.h>

namespace pegasus {
namespace server {

class pegasus_server_test_base : public ::testing::Test
{
public:
    pegasus_server_test_base()
    {
        // Remove rdb to prevent rocksdb recovery from last test.
        dsn::utils::filesystem::remove_path("./data/rdb");
        _replica_stub = dsn::replication::create_test_replica_stub();

        _gpid = dsn::gpid(100, 1);
        dsn::app_info app_info;
        app_info.app_type = "pegasus";

        _replica =
            dsn::replication::create_test_replica(_replica_stub, _gpid, app_info, "./", false);

        _server = dsn::make_unique<pegasus_server_impl>(_replica);
    }

    dsn::error_code start(const std::map<std::string, std::string> &envs = {})
    {
        std::unique_ptr<char *[]> argvs = dsn::make_unique<char *[]>(envs.size() * 2);
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

        dsn::replication::destroy_replica_stub(_replica_stub);
        dsn::replication::destroy_replica(_replica);
    }

protected:
    std::unique_ptr<pegasus_server_impl> _server;
    dsn::replication::replica *_replica;
    dsn::replication::replica_stub *_replica_stub;
    dsn::gpid _gpid;
};

} // namespace server
} // namespace pegasus
