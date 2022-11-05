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

#include "runtime/tool_api.h"
#include "runtime/rpc/rpc_stream.h"

#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TVirtualProtocol.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/TApplicationException.h>
#include <type_traits>

using namespace ::apache::thrift::transport;
namespace dsn {

class binary_reader_transport : public TVirtualTransport<binary_reader_transport>
{
public:
    binary_reader_transport(binary_reader &reader) : _reader(reader) {}

    bool isOpen() { return true; }

    void open() {}

    void close() {}

    uint32_t read(uint8_t *buf, uint32_t len)
    {
        int l = _reader.read((char *)buf, static_cast<int>(len));
        if (dsn_unlikely(l <= 0)) {
            throw TTransportException(TTransportException::END_OF_FILE,
                                      "no more data to read after end-of-buffer");
        }
        return (uint32_t)l;
    }

private:
    binary_reader &_reader;
};

class binary_writer_transport : public TVirtualTransport<binary_writer_transport>
{
public:
    binary_writer_transport(binary_writer &writer) : _writer(writer) {}

    bool isOpen() { return true; }

    void open() {}

    void close() {}

    void write(const uint8_t *buf, uint32_t len)
    {
        _writer.write((const char *)buf, static_cast<int>(len));
    }

private:
    binary_writer &_writer;
};

#define DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(TName, TRealName, TTag, TMethod)                     \
    inline uint32_t write_base(::apache::thrift::protocol::TProtocol *proto, const TName &val)     \
    {                                                                                              \
        return proto->write##TMethod((const TRealName &)val);                                      \
    }                                                                                              \
    inline uint32_t read_base(::apache::thrift::protocol::TProtocol *proto, /*out*/ TName &val)    \
    {                                                                                              \
        return proto->read##TMethod((TRealName &)val);                                             \
    }

DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(bool, bool, BOOL, Bool)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int8_t, int8_t, I08, Byte)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int16_t, int16_t, I16, I16)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int32_t, int32_t, I32, I32)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int64_t, int64_t, I64, I64)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(uint8_t, int8_t, I08, Byte)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(uint16_t, int16_t, I16, I16)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(uint32_t, int32_t, I32, I32)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(uint64_t, int64_t, I64, I64)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(double, double, DOUBLE, Double)
DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(std::string, std::string, STRING, String)

template <typename T>
uint32_t marshall_base(::apache::thrift::protocol::TProtocol *oproto, const T &val);
template <typename T>
uint32_t unmarshall_base(::apache::thrift::protocol::TProtocol *iproto, T &val);

template <typename T>
inline uint32_t write_base(::apache::thrift::protocol::TProtocol *oprot, const std::vector<T> &val)
{
    uint32_t xfer = oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT,
                                          static_cast<uint32_t>(val.size()));
    for (auto iter = val.begin(); iter != val.end(); ++iter) {
        marshall_base(oprot, *iter);
    }
    xfer += oprot->writeListEnd();
    return xfer;
}

template <typename T>
inline uint32_t read_base(::apache::thrift::protocol::TProtocol *iprot, std::vector<T> &val)
{
    uint32_t xfer = 0;

    val.clear();
    uint32_t size;
    ::apache::thrift::protocol::TType element_type;
    xfer += iprot->readListBegin(element_type, size);
    val.resize(size);
    for (uint32_t i = 0; i != size; ++i) {
        xfer += unmarshall_base(iprot, val[i]);
    }
    xfer += iprot->readListEnd();

    return xfer;
}

template <typename T_KEY, typename T_VALUE>
inline uint32_t write_base(::apache::thrift::protocol::TProtocol *oprot,
                           const std::map<T_KEY, T_VALUE> &val)
{
    uint32_t xfer = 0;

    xfer += oprot->writeMapBegin(::apache::thrift::protocol::T_STRUCT,
                                 ::apache::thrift::protocol::T_STRUCT,
                                 static_cast<uint32_t>(val.size()));
    for (auto iter = val.begin(); iter != val.end(); ++iter) {
        xfer += marshall_base(oprot, iter->first);
        xfer += marshall_base(oprot, iter->second);
    }
    xfer += oprot->writeMapEnd();

    return xfer;
}

template <typename T_KEY, typename T_VALUE>
inline uint32_t read_base(::apache::thrift::protocol::TProtocol *iprot,
                          std::map<T_KEY, T_VALUE> &val)
{
    int xfer = 0;

    uint32_t size;
    ::apache::thrift::protocol::TType ktype;
    ::apache::thrift::protocol::TType vtype;
    xfer += iprot->readMapBegin(ktype, vtype, size);
    for (uint32_t i = 0; i < size; ++i) {
        T_KEY mkey;
        xfer += unmarshall_base(iprot, mkey);
        T_VALUE &mval = val[mkey];
        xfer += unmarshall_base(iprot, mval);
    }
    xfer += iprot->readMapEnd();

    return xfer;
}

inline uint32_t rpc_address::read(apache::thrift::protocol::TProtocol *iprot)
{
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        auto r = iprot->readI64(reinterpret_cast<int64_t &>(_addr.value));
        CHECK(_addr.v4.type == HOST_TYPE_INVALID || _addr.v4.type == HOST_TYPE_IPV4,
              "only invalid or ipv4 can be deserialized from binary");
        return r;
    } else {
        // the protocol is json protocol
        std::string host;
        int port = 0;

        uint32_t xfer = 0;
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        xfer += iprot->readStructBegin(fname);

        using ::apache::thrift::protocol::TProtocolException;

        while (true) {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid) {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRING) {
                    xfer += iprot->readString(host);
                } else {
                    xfer += iprot->skip(ftype);
                }
                break;
            case 2:
                if (ftype == ::apache::thrift::protocol::T_I32) {
                    xfer += iprot->readI32(port);
                } else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }

        xfer += iprot->readStructEnd();

        // currently only support ipv4 format
        this->assign_ipv4(host.c_str(), port);

        return xfer;
    }
}

inline uint32_t rpc_address::write(apache::thrift::protocol::TProtocol *oprot) const
{
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(oprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        CHECK(_addr.v4.type == HOST_TYPE_INVALID || _addr.v4.type == HOST_TYPE_IPV4,
              "only invalid or ipv4 can be serialized to binary");
        return oprot->writeI64((int64_t)_addr.value);
    } else {
        // the protocol is json protocol
        std::string host(this->to_string());
        int port = 0;
        size_t sep_index = host.find(':');
        if (sep_index != std::string::npos) {
            port = std::stoi(host.substr(sep_index + 1));
            host = host.substr(0, sep_index);
        }

        uint32_t xfer = 0;

        xfer += oprot->writeStructBegin("rpc_address");

        xfer += oprot->writeFieldBegin("host", ::apache::thrift::protocol::T_STRING, 1);
        xfer += oprot->writeString(host);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("port", ::apache::thrift::protocol::T_I32, 2);
        xfer += oprot->writeI32(port);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
}

inline uint32_t gpid::read(apache::thrift::protocol::TProtocol *iprot)
{
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        return iprot->readI64(reinterpret_cast<int64_t &>(_value.value));
    } else {
        // the protocol is json protocol
        uint32_t xfer = 0;
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        xfer += iprot->readStructBegin(fname);

        using ::apache::thrift::protocol::TProtocolException;

        while (true) {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid) {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_I64) {
                    xfer += iprot->readI64(reinterpret_cast<int64_t &>(_value.value));
                } else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }

        xfer += iprot->readStructEnd();

        return xfer;
    }
}

inline uint32_t gpid::write(apache::thrift::protocol::TProtocol *oprot) const
{
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(oprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        return oprot->writeI64((int64_t)_value.value);
    } else {
        // the protocol is json protocol
        uint32_t xfer = 0;

        xfer += oprot->writeStructBegin("gpid");

        xfer += oprot->writeFieldBegin("id", ::apache::thrift::protocol::T_I64, 1);
        xfer += oprot->writeI64((int64_t)_value.value);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
}

inline uint32_t task_code::read(apache::thrift::protocol::TProtocol *iprot)
{
    std::string task_code_string;
    uint32_t xfer = 0;
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        xfer += iprot->readString(task_code_string);
    } else {
        // the protocol is json protocol
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        xfer += iprot->readStructBegin(fname);

        using ::apache::thrift::protocol::TProtocolException;

        while (true) {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid) {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRING) {
                    xfer += iprot->readString(task_code_string);
                } else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }

        xfer += iprot->readStructEnd();
    }
    _internal_code = try_get(task_code_string, TASK_CODE_INVALID);
    return xfer;
}

inline uint32_t task_code::write(apache::thrift::protocol::TProtocol *oprot) const
{
    const char *name = to_string();
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(oprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        return binary_proto->writeString(string_view(name));
    } else {
        // the protocol is json protocol
        uint32_t xfer = 0;
        xfer += oprot->writeStructBegin("task_code");

        xfer += oprot->writeFieldBegin("code", ::apache::thrift::protocol::T_STRING, 1);
        xfer += oprot->writeString(std::string(name));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
}

inline uint32_t error_code::read(apache::thrift::protocol::TProtocol *iprot)
{
    std::string ec_string;
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    uint32_t xfer = 0;
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        xfer += iprot->readString(ec_string);
    } else {
        // the protocol is json protocol
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        xfer += iprot->readStructBegin(fname);

        using ::apache::thrift::protocol::TProtocolException;

        while (true) {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid) {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRING) {
                    xfer += iprot->readString(ec_string);
                } else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }

        xfer += iprot->readStructEnd();
    }
    *this = error_code::try_get(ec_string, ERR_UNKNOWN);
    return xfer;
}

inline uint32_t error_code::write(apache::thrift::protocol::TProtocol *oprot) const
{
    const char *name = to_string();
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(oprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        return binary_proto->writeString(string_view(name));
    } else {
        // the protocol is json protocol
        uint32_t xfer = 0;
        xfer += oprot->writeStructBegin("error_code");

        xfer += oprot->writeFieldBegin("code", ::apache::thrift::protocol::T_STRING, 1);
        xfer += oprot->writeString(std::string(name));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
}

inline const char *to_string(const rpc_address &addr) { return addr.to_string(); }
inline const char *to_string(const blob &blob) { return ""; }
inline const char *to_string(const task_code &code) { return code.to_string(); }
inline const char *to_string(const error_code &ec) { return ec.to_string(); }
inline const char *to_string(const gpid &id)
{
    static char str[64];
    snprintf(str, 64, "%d.%d", id.get_app_id(), id.get_partition_index());
    return str;
}

template <typename T>
class serialization_forwarder
{
private:
    template <typename C>
    static constexpr auto check_method(C *) -> typename std::is_same<
        decltype(std::declval<C>().write(std::declval<::apache::thrift::protocol::TProtocol *>())),
        uint32_t>::type;

    template <typename>
    static constexpr std::false_type check_method(...);

    typedef decltype(check_method<T>(nullptr)) has_read_write_method;

    static uint32_t marshall_internal(::apache::thrift::protocol::TProtocol *oproto,
                                      const T &value,
                                      std::false_type)
    {
        return write_base(oproto, value);
    }

    static uint32_t
    marshall_internal(::apache::thrift::protocol::TProtocol *oproto, const T &value, std::true_type)
    {
        return value.write(oproto);
    }

    static uint32_t
    unmarshall_internal(::apache::thrift::protocol::TProtocol *iproto, T &value, std::false_type)
    {
        return read_base(iproto, value);
    }

    static uint32_t
    unmarshall_internal(::apache::thrift::protocol::TProtocol *iproto, T &value, std::true_type)
    {
        return value.read(iproto);
    }

public:
    static uint32_t marshall(::apache::thrift::protocol::TProtocol *oproto, const T &value)
    {
        return marshall_internal(oproto, value, has_read_write_method());
    }

    static uint32_t unmarshall(::apache::thrift::protocol::TProtocol *iproto, T &value)
    {
        return unmarshall_internal(iproto, value, has_read_write_method());
    }
};

template <typename TName>
inline uint32_t marshall_base(::apache::thrift::protocol::TProtocol *oproto, const TName &val)
{
    return serialization_forwarder<TName>::marshall(oproto, val);
}

template <typename TName>
inline uint32_t unmarshall_base(::apache::thrift::protocol::TProtocol *iproto, /*out*/ TName &val)
{
    // well, we assume read/write are in coupled
    return serialization_forwarder<TName>::unmarshall(iproto, val);
}

#define GET_THRIFT_TYPE_MACRO(cpp_type, thrift_type)                                               \
    inline ::apache::thrift::protocol::TType get_thrift_type(const cpp_type &)                     \
    {                                                                                              \
        return ::apache::thrift::protocol::thrift_type;                                            \
    }

GET_THRIFT_TYPE_MACRO(bool, T_BOOL)
GET_THRIFT_TYPE_MACRO(int8_t, T_BYTE)
GET_THRIFT_TYPE_MACRO(uint8_t, T_BYTE)
GET_THRIFT_TYPE_MACRO(int16_t, T_I16)
GET_THRIFT_TYPE_MACRO(uint16_t, T_I16)
GET_THRIFT_TYPE_MACRO(int32_t, T_I32)
GET_THRIFT_TYPE_MACRO(uint32_t, T_I32)
GET_THRIFT_TYPE_MACRO(int64_t, T_I64)
GET_THRIFT_TYPE_MACRO(uint64_t, T_U64)
GET_THRIFT_TYPE_MACRO(double, T_DOUBLE)
GET_THRIFT_TYPE_MACRO(std::string, T_STRING)

template <typename T>
inline ::apache::thrift::protocol::TType get_thrift_type(const std::vector<T> &)
{
    return ::apache::thrift::protocol::T_LIST;
}

template <typename T>
inline ::apache::thrift::protocol::TType get_thrift_type(const T &)
{
    return ::apache::thrift::protocol::T_STRUCT;
}

template <typename T>
inline void marshall_thrift_internal(const T &val, ::apache::thrift::protocol::TProtocol *proto)
{
    /*
     * we treat every element as a whole struct
     */
    proto->writeStructBegin("thrift_rpc_result");
    proto->writeFieldBegin("success", get_thrift_type(val), 0);
    marshall_base<T>(proto, val);
    proto->writeFieldEnd();
    proto->writeFieldStop();
    proto->writeStructEnd();
}

template <typename T>
inline void unmarshall_thrift_internal(T &val, ::apache::thrift::protocol::TProtocol *proto)
{
    std::string fname;
    ::apache::thrift::protocol::TType ftype;
    int16_t fid;
    proto->readStructBegin(fname);

    // read the struct
    proto->readFieldBegin(fname, ftype, fid);
    unmarshall_base<T>(proto, val);
    proto->readFieldEnd();

    // read the stop
    proto->readFieldBegin(fname, ftype, fid);

    proto->readStructEnd();
}

template <typename T>
inline void marshall_thrift_binary(binary_writer &writer, const T &val)
{
    ::dsn::binary_writer_transport trans(writer);
    boost::shared_ptr<::dsn::binary_writer_transport> transport(
        &trans, [](::dsn::binary_writer_transport *) {});
    ::apache::thrift::protocol::TBinaryProtocol proto(transport);
    marshall_thrift_internal(val, &proto);
    proto.getTransport()->flush();
}

template <typename T>
inline void marshall_thrift_json(binary_writer &writer, const T &val)
{
    ::dsn::binary_writer_transport trans(writer);
    boost::shared_ptr<::dsn::binary_writer_transport> transport(
        &trans, [](::dsn::binary_writer_transport *) {});
    ::apache::thrift::protocol::TJSONProtocol proto(transport);
    marshall_thrift_internal(val, &proto);
    proto.getTransport()->flush();
}

template <typename T>
inline void unmarshall_thrift_binary(binary_reader &reader, T &val)
{
    ::dsn::binary_reader_transport trans(reader);
    boost::shared_ptr<::dsn::binary_reader_transport> transport(
        &trans, [](::dsn::binary_reader_transport *) {});
    ::apache::thrift::protocol::TBinaryProtocol proto(transport);
    unmarshall_thrift_internal(val, &proto);
}

template <typename T>
inline void unmarshall_thrift_json(binary_reader &reader, T &val)
{
    ::dsn::binary_reader_transport trans(reader);
    boost::shared_ptr<::dsn::binary_reader_transport> transport(
        &trans, [](::dsn::binary_reader_transport *) {});
    ::apache::thrift::protocol::TJSONProtocol proto(transport);
    unmarshall_thrift_internal(val, &proto);
}
} // namespace dsn
