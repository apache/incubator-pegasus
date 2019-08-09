// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdio>
#include <zconf.h>
#include <pegasus/client.h>

#include "benchmark.h"

namespace google {
}
using namespace google;
using namespace pegasus;

static const std::string pegasus_config = "config.ini";
int db_bench_tool(int argc, char **argv)
{
    bool init = pegasus::pegasus_client_factory::initialize(pegasus_config.c_str());
    if (!init) {
        fprintf(stderr, "Init pegasus error\n");
        return -1;
    }
    sleep(1);
    fprintf(stdout, "Init pegasus succeed\n");

    pegasus::test::benchmark bm;
    bm.run();
    sleep(1); // Sleep a while to exit gracefully.

    return 0;
}

int main(int argc, char **argv) { return db_bench_tool(argc, argv); }
