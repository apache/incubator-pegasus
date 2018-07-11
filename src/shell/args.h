// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/c/app_model.h>
#include <dsn/utility/defer.h>

#include "linenoise/linenoise.h"
#include "sds/sds.h"

inline sds *scanfCommand(int *argc)
{
    char *line = nullptr;
    auto _ = dsn::defer([line]() {
        if (line) {
            linenoiseFree(line);
        }
    });

    if ((line = linenoise(">>> ")) == nullptr) {
        dsn_exit(0);
    }
    if (line[0] == '\0') {
        return nullptr;
    }
    linenoiseHistoryAdd(line);
    linenoiseHistorySave(".shell-history");

    return sdssplitargs(line, argc);
}

inline std::string sds_to_string(const sds s) { return std::string(s, sdslen(s)); }
