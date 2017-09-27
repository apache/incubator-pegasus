// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <pegasus/client.h>
#include "pegasus_client_factory_impl.h"

namespace pegasus {

bool pegasus_client_factory::initialize(const char *config_file)
{
    return client::pegasus_client_factory_impl::initialize(config_file);
}

pegasus_client *pegasus_client_factory::get_client(const char *cluster_name, const char *app_name)
{
    return client::pegasus_client_factory_impl::get_client(cluster_name, app_name);
}

} // namespace
