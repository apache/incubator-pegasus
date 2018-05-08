// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/string_view.h>
#include <rocksdb/slice.h>

namespace pegasus {

inline dsn::string_view to_string_view(rocksdb::Slice s) { return {s.data(), s.size()}; }

inline rocksdb::Slice to_rocksdb_slice(dsn::string_view s) { return {s.data(), s.size()}; }

} // namespace pegasus
