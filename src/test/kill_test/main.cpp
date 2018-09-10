// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdio>
#include <cstring>

#include "data_verifier.h"
#include "process_kill_testor.h"
#include "partition_kill_testor.h"

int main(int argc, const char **argv)
{
    if (argc < 3) {
        printf("invalid arguments: pegasus_kill_test configfile "
               "worker_type(verifier|process_killer|partition_killer)\n");
        return -1;
    } else if (strcmp(argv[2], "verifier") == 0) {
        verifier_initialize(argv[1]);
        verifier_start();
    } else if (strcmp(argv[2], "process_killer") == 0) {
        pegasus::test::kill_testor *killtestor = new pegasus::test::process_kill_testor(argv[1]);
        killtestor->Run();
    } else if (strcmp(argv[2], "partition_killer") == 0) {
        pegasus::test::kill_testor *killtestor = new pegasus::test::partition_kill_testor(argv[1]);
        killtestor->Run();
    } else {
        printf("invalid worker_type: %s\n", argv[2]);
        return -1;
    }

    return 0;
}
