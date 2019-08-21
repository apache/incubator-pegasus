// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <unistd.h>
#include <pegasus/client.h>
#include <dsn/dist/fmt_logging.h>

#include "benchmark.h"

int db_bench_tool(const char *config_file)
{
    bool init = pegasus::pegasus_client_factory::initialize(config_file);
    if (!init) {
        fmt::print(stderr, "Init pegasus error\n");
        return -1;
    }
    sleep(1);
    fmt::print(stdout, "Init pegasus succeed\n");

    pegasus::test::benchmark bm;
    bm.run();
    sleep(1); // Sleep a while to exit gracefully.

    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fmt::print(stderr, "USAGE: {} <config-file>", argv[0]);
        return -1;
    }

    return db_bench_tool(argv[1]);
}
