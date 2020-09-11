// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/cpp/rpc_holder.h>
#include <dsn/dist/replication/replication_types.h>
#include <rrdb/rrdb_types.h>
#include <rrdb/rrdb.client.h>

namespace pegasus {

using multi_put_rpc = dsn::rpc_holder<dsn::apps::multi_put_request, dsn::apps::update_response>;

using put_rpc = dsn::rpc_holder<dsn::apps::update_request, dsn::apps::update_response>;

using multi_remove_rpc =
    dsn::rpc_holder<dsn::apps::multi_remove_request, dsn::apps::multi_remove_response>;

using remove_rpc = dsn::rpc_holder<dsn::blob, dsn::apps::update_response>;

using incr_rpc = dsn::rpc_holder<dsn::apps::incr_request, dsn::apps::incr_response>;

using check_and_set_rpc =
    dsn::rpc_holder<dsn::apps::check_and_set_request, dsn::apps::check_and_set_response>;

using duplicate_rpc = dsn::apps::duplicate_rpc;

using check_and_mutate_rpc =
    dsn::rpc_holder<dsn::apps::check_and_mutate_request, dsn::apps::check_and_mutate_response>;

using ingestion_rpc =
    dsn::rpc_holder<dsn::replication::ingestion_request, dsn::replication::ingestion_response>;

using detect_hotkey_rpc =
    dsn::rpc_holder<dsn::apps::hotkey_detect_request, dsn::apps::hotkey_detect_response>;

} // namespace pegasus
