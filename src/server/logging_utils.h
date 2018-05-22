// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/fmt_logging.h>

/// Utilities for logging the operation on rocksdb.

#define derror_rocksdb(op, error, ...)                                                             \
    derror_f("{}: rocksdb {} failed: error = {} [{}]",                                             \
             replica_name(),                                                                       \
             op,                                                                                   \
             error,                                                                                \
             fmt::format(__VA_ARGS__))

#define ddebug_rocksdb(op, ...)                                                                    \
    ddebug_f("{}: rocksdb {}: [{}]", replica_name(), op, fmt::format(__VA_ARGS__))

#define dwarn_rocksdb(op, ...)                                                                     \
    dwarn_f("{}: rocksdb {}: [{}]", replica_name(), op, fmt::format(__VA_ARGS__))
