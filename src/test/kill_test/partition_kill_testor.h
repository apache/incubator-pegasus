// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/cli/cli.client.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <pegasus/client.h>

#include "kill_testor.h"
#include "killer_registry.h"

namespace pegasus {
namespace test {
class partition_kill_testor : public kill_testor
{
public:
    partition_kill_testor(const char *config_file);
    ~partition_kill_testor();

    virtual void Run();

private:
    void run();

    ::dsn::command cmd;
};
} // namespace test
} // namespace pegasus
