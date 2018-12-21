/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <dsn/utility/string_view.h>
#include <dsn/utility/binary_writer.h>
#include <dsn/utility/binary_reader.h>
#include <dsn/cpp/rpc_stream.h>
#include <dsn/cpp/serialization.h>
#include <dsn/tool-api/rpc_message.h>
#include <dsn/cpp/serialization_helper/dsn.layer2_types.h>

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
extern message_ex *
from_blob_to_received_msg(task_code rpc_code,
                          const blob &bb,
                          int thread_hash = 0,
                          uint64_t partition_hash = 0,
                          dsn_msg_serialize_format serialization_type = DSF_THRIFT_BINARY);
inline message_ex *
from_blob_to_received_msg(task_code rpc_code,
                          blob &&bb,
                          int thread_hash = 0,
                          uint64_t partition_hash = 0,
                          dsn_msg_serialize_format serialization_type = DSF_THRIFT_BINARY)
{
    return from_blob_to_received_msg(rpc_code, bb, thread_hash, partition_hash, serialization_type);
}

/// Convert a thrift request into a dsn message (using binary encoding).
/// It's useful for unit test, especially when we need to create a fake message
/// as test input.
template <typename T>
inline message_ex *from_thrift_request_to_received_message(const T &thrift_request, task_code tc)
{
    binary_writer writer;
    marshall_thrift_binary(writer, thrift_request);
    return from_blob_to_received_msg(tc, writer.get_buffer());
}

/// Convert a blob into a thrift object.
template <typename T>
inline void from_blob_to_thrift(const blob &data, T &thrift_obj)
{
    binary_reader reader(data);
    unmarshall_thrift_binary(reader, thrift_obj);
}

} // namespace dsn
