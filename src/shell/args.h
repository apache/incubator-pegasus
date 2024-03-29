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

#include <string>

#include "linenoise/linenoise.h"
#include "runtime/app_model.h"
#include "sds/sds.h"
#include "utils/defer.h"

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
