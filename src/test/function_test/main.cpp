// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdlib>
#include <string>
#include <vector>
#include <map>

#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/service_api_c.h>
#include <unistd.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>
#include "base/pegasus_const.h"

using namespace ::dsn;
using namespace ::replication;
using namespace ::pegasus;

pegasus_client *client = nullptr;
std::shared_ptr<replication_ddl_client> ddl_client;

GTEST_API_ int main(int argc, char **argv)
{
    if (argc < 3) {
        derror("USAGE: %s <config-file> <app-name> [gtest args...]", argv[0]);
        return -1;
    }

    const char *config_file = argv[1];
    if (!pegasus_client_factory::initialize(config_file)) {
        derror("MainThread: init pegasus failed");
        return -1;
    }

    const char *app_name = argv[2];
    client = pegasus_client_factory::get_client("mycluster", app_name);
    std::vector<rpc_address> meta_list;
    replica_helper::load_meta_servers(meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster");
    ddl_client = std::make_shared<replication_ddl_client>(meta_list);
    ddebug("MainThread: app_name=%s", app_name);

    int gargc = argc - 2;
    testing::InitGoogleTest(&gargc, argv + 2);
    int ans = RUN_ALL_TESTS();
    dsn_exit(ans);
}
