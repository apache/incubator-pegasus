// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/c/app_model.h>

#include "linenoise/linenoise.h"
#include "sds/sds.h"

template <typename Func>
struct deferred_action
{
    explicit deferred_action(Func &&func) noexcept : _func(std::move(func)) {}
    ~deferred_action() { _func(); }
private:
    Func _func;
};

template <typename Func>
inline deferred_action<Func> defer(Func &&func)
{
    return deferred_action<Func>(std::forward<Func>(func));
}

inline sds *scanfCommand(int *argc)
{
    char *line = NULL;
    auto _ = defer([line]() {
        if (line) {
            linenoiseFree(line);
        }
    });

    if ((line = linenoise(">>> ")) == NULL) {
        dsn_exit(0);
    }
    if (line[0] == '\0') {
        return NULL;
    }
    linenoiseHistoryAdd(line);
    linenoiseHistorySave(".shell-history");

    return sdssplitargs(line, argc);
}
