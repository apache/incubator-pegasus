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

#include <cstdio>
#include <cstring>

#include "data_verifier.h"
#include "process_kill_testor.h"
#include "partition_kill_testor.h"
#include "utils/strings.h"

int main(int argc, const char **argv)
{
    if (argc < 3) {
        printf("invalid arguments: pegasus_kill_test configfile "
               "worker_type(verifier|process_killer|partition_killer)\n");
        return -1;
    } else if (dsn::utils::equals(argv[2], "verifier")) {
        pegasus::test::verifier_initialize(argv[1]);
        pegasus::test::verifier_start();
    } else if (dsn::utils::equals(argv[2], "process_killer")) {
        pegasus::test::kill_testor *killtestor = new pegasus::test::process_kill_testor(argv[1]);
        killtestor->Run();
    } else if (dsn::utils::equals(argv[2], "partition_killer")) {
        pegasus::test::kill_testor *killtestor = new pegasus::test::partition_kill_testor(argv[1]);
        killtestor->Run();
    } else {
        printf("invalid worker_type: %s\n", argv[2]);
        return -1;
    }

    return 0;
}
