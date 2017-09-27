// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/tool_api.h>
#include <dsn/utility/singleton.h>

namespace pegasus {
namespace server {

::dsn::perf_counter *pegasus_perf_counter_factory(const char *app,
                                                  const char *section,
                                                  const char *name,
                                                  dsn_perf_counter_type_t type,
                                                  const char *dsptr);
}
} // namespace
