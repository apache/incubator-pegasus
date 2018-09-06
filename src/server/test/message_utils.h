// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "rrdb/rrdb_types.h"
#include "rrdb/rrdb.code.definition.h"

#include <dsn/cpp/message_utils.h>

namespace pegasus {

inline dsn::message_ex *create_multi_put_request(const dsn::apps::multi_put_request &request)
{
    return dsn::from_thrift_request_to_received_message(request,
                                                        dsn::apps::RPC_RRDB_RRDB_MULTI_PUT);
}

inline dsn::message_ex *create_multi_remove_request(const dsn::apps::multi_remove_request &request)
{
    return dsn::from_thrift_request_to_received_message(request,
                                                        dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE);
}

inline dsn::message_ex *create_put_request(const dsn::apps::update_request &request)
{
    return dsn::from_thrift_request_to_received_message(request, dsn::apps::RPC_RRDB_RRDB_PUT);
}

inline dsn::message_ex *create_remove_request(const dsn::blob &key)
{
    return dsn::from_thrift_request_to_received_message(key, dsn::apps::RPC_RRDB_RRDB_REMOVE);
}

inline dsn::message_ex *create_incr_request(const dsn::apps::incr_request &request)
{
    return dsn::from_thrift_request_to_received_message(request, dsn::apps::RPC_RRDB_RRDB_INCR);
}

} // namespace pegasus
