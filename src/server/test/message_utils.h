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

#include "rrdb/rrdb_types.h"
#include "rrdb/rrdb.code.definition.h"

#include "runtime/message_utils.h"

namespace pegasus {

inline message_ex *create_multi_put_request(const apps::multi_put_request &request)
{
    return from_thrift_request_to_received_message(request, apps::RPC_RRDB_RRDB_MULTI_PUT);
}

inline message_ex *create_multi_remove_request(const apps::multi_remove_request &request)
{
    return from_thrift_request_to_received_message(request, apps::RPC_RRDB_RRDB_MULTI_REMOVE);
}

inline message_ex *create_put_request(const apps::update_request &request)
{
    return from_thrift_request_to_received_message(request, apps::RPC_RRDB_RRDB_PUT);
}

inline message_ex *create_remove_request(const blob &key)
{
    return from_thrift_request_to_received_message(key, apps::RPC_RRDB_RRDB_REMOVE);
}

inline message_ex *create_incr_request(const apps::incr_request &request)
{
    return from_thrift_request_to_received_message(request, apps::RPC_RRDB_RRDB_INCR);
}

} // namespace pegasus
