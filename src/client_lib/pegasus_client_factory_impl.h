// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <pegasus/client.h>
#include <pegasus/error.h>
#include "pegasus_client_impl.h"

namespace pegasus {
namespace client {

class pegasus_client_factory_impl
{
public:
    static bool initialize(const char *config_file);

    static pegasus_client *get_client(const char *cluster_name, const char *app_name);

private:
    typedef std::unordered_map<std::string, pegasus_client_impl *> app_to_client_map;
    typedef std::unordered_map<std::string, app_to_client_map> cluster_to_app_map;
    static cluster_to_app_map _cluster_to_clients;
    static ::dsn::zlock *_map_lock;
};
}
} // namespace
