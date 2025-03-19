// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <cstring>
#include <utility>

#include "rpc/rpc_message.h"
#include "rpc/rpc_stream.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "thrift_helper.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"

namespace dsn {

/// Move the content inside message `m` into a blob.
inline blob move_message_to_blob(message_ex *m)
{
    rpc_read_stream reader(m);
    return reader.get_buffer();
}

/// Convert a blob into a message for reading(unmarshalling).
/// This function is identical with dsn::message_ex::create_received_request,
/// however it passes a blob to ensure ownership safety instead of
/// passing simply a constant view.
/// MUST released manually later using dsn::message_ex::release_ref.
template <typename TBlob>
inline message_ex *from_blob_to_received_msg(task_code rpc_code,
                                             TBlob &&bb,
                                             int thread_hash,
                                             uint64_t partition_hash,
                                             dsn_msg_serialize_format serialization_type)
{
    auto msg = message_ex::create_receive_message_with_standalone_header(std::forward<TBlob>(bb));
    msg->local_rpc_code = rpc_code;
    const char *name = rpc_code.to_string();
    strncpy(msg->header->rpc_name, name, sizeof(msg->header->rpc_name) - 1);
    msg->header->rpc_name[sizeof(msg->header->rpc_name) - 1] = '\0';

    msg->header->client.thread_hash = thread_hash;
    msg->header->client.partition_hash = partition_hash;
    msg->header->context.u.serialize_format = serialization_type;
    return msg;
}

template <typename TBlob>
inline message_ex *from_blob_to_received_msg(task_code rpc_code, TBlob &&bb)
{
    return from_blob_to_received_msg(rpc_code, std::forward<TBlob>(bb), 0, 0, DSF_THRIFT_BINARY);
}

/// Convert a thrift request into a dsn message (using binary encoding).
/// It's useful for unit test, especially when we need to create a fake message
/// as test input.
template <typename T>
inline message_ex *
from_thrift_request_to_received_message(const T &thrift_request,
                                        task_code rpc_code,
                                        int thread_hash,
                                        uint64_t partition_hash,
                                        dsn_msg_serialize_format serialization_type)
{
    binary_writer writer;
    marshall_thrift_binary(writer, thrift_request);
    return from_blob_to_received_msg(
        rpc_code, writer.get_buffer(), thread_hash, partition_hash, serialization_type);
}

template <typename T>
inline message_ex *from_thrift_request_to_received_message(const T &thrift_request,
                                                           task_code rpc_code)
{
    return from_thrift_request_to_received_message(
        thrift_request, rpc_code, 0, 0, DSF_THRIFT_BINARY);
}

/// Convert a blob into a thrift object.
template <typename T>
inline void from_blob_to_thrift(const blob &data, T &thrift_obj)
{
    binary_reader reader(data);
    unmarshall_thrift_binary(reader, thrift_obj);
}

} // namespace dsn
