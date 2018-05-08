// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <pegasus/client.h>
#include <dsn/service_api_c.h>
#include <dsn/utility/string_view.h>

namespace pegasus {
namespace test {

// We must run init_pegasus_client before everything. It sets up the underlying
// rDSN environment and initializes a global pegasus client.
inline void init_pegasus_client()
{
    if (!pegasus_client_factory::initialize("config.ini")) {
        dfatal("failed to initialize pegasus client");
    }
}

inline pegasus_client *must_get_client(dsn::string_view cluster_name, dsn::string_view app_name)
{
    auto c = pegasus_client_factory::get_client(cluster_name.data(), app_name.data());
    if (c == nullptr) {
        dfatal("failed to create pegasus client for [cluster: %s, app: %s]",
               cluster_name.data(),
               app_name.data());
        __builtin_unreachable();
    }
    return c;
}

} // namespace test
} // namespace pegasus
