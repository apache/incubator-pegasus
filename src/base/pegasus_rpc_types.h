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

#include "rpc/rpc_holder.h"
#include "meta_admin_types.h"
#include "partition_split_types.h"
#include "duplication_types.h"
#include "bulk_load_types.h"
#include "backup_types.h"
#include "consensus_types.h"
#include "replica_admin_types.h"
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

} // namespace pegasus
