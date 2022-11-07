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

#include "utils/utils.h"
#include "utils/rpc_address.h"
#include "runtime/rpc/rpc_stream.h"
#include "common/serialization_helper/thrift_helper.h"

namespace dsn {
namespace serialization {

template <typename T>
std::string no_registered_function_error_notice(const T &t, dsn_msg_serialize_format fmt)
{
    std::stringstream ss;
    ss << "This error occurs because someone is trying to ";
    ss << "serialize/deserialize an object of the type ";
    ss << typeid(t).name();
    ss << " but has not registered corresponding serialization/deserialization function for the "
          "format of ";
    //           ss << enum_to_string(fmt) << ".";
    ss << fmt << ".";
    return ss.str();
}

} // namespace serialization

template <typename ThriftType>
inline void marshall(binary_writer &writer, const ThriftType &value, dsn_msg_serialize_format fmt)
{
    switch (fmt) {
    case DSF_THRIFT_BINARY:
        marshall_thrift_binary(writer, value);
        break;
    case DSF_THRIFT_JSON:
        marshall_thrift_json(writer, value);
        break;
    default:
        CHECK(false, serialization::no_registered_function_error_notice(value, fmt));
    }
}

template <typename ThriftType>
inline void unmarshall(binary_reader &reader, ThriftType &value, dsn_msg_serialize_format fmt)
{
    switch (fmt) {
    case DSF_THRIFT_BINARY:
        unmarshall_thrift_binary(reader, value);
        break;
    case DSF_THRIFT_JSON:
        unmarshall_thrift_json(reader, value);
        break;
    default:
        CHECK(false, serialization::no_registered_function_error_notice(value, fmt));
    }
}

template <typename T>
inline void marshall(dsn::message_ex *msg, const T &val)
{
    ::dsn::rpc_write_stream writer(msg);
    marshall(writer, val, (dsn_msg_serialize_format)msg->header->context.u.serialize_format);
}

template <typename T>
inline void marshall(dsn::message_ex *msg, const T &val, dsn_msg_serialize_format fmt)
{
    ::dsn::rpc_write_stream writer(msg);
    marshall(writer, val, fmt);
}

template <typename T>
inline void unmarshall(dsn::message_ex *msg, /*out*/ T &val)
{
    ::dsn::rpc_read_stream reader(msg);
    unmarshall(reader, val, (dsn_msg_serialize_format)msg->header->context.u.serialize_format);
}

} // namespace dsn
