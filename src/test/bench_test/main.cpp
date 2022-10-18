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

#include <unistd.h>
#include <pegasus/client.h>
#include "utils/fmt_logging.h"
#include "runtime/app_model.h"

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

    auto result = db_bench_tool(argv[1]);
    dsn_exit(result);
}
