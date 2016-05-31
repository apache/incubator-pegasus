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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     2016-02-24, Weijie Sun(sunweijie[at]xiaomi.com), add support for serialization in thrift
 *     2016-03-01, Weijie Sun(sunweijie[at]xiaomi.com), add support for rpc in thrift
 */

# pragma once

# include <dsn/tool_api.h>
# include <dsn/cpp/rpc_stream.h>

# include <thrift/Thrift.h>
# include <thrift/protocol/TBinaryProtocol.h>
# include <thrift/protocol/TJSONProtocol.h>
# include <thrift/protocol/TVirtualProtocol.h>
# include <thrift/transport/TVirtualTransport.h>
# include <thrift/TApplicationException.h>
# include <type_traits>

using namespace ::apache::thrift::transport;
namespace dsn {

    class binary_reader_transport : public TVirtualTransport<binary_reader_transport>
    {
    public:
        binary_reader_transport(binary_reader& reader)
            : _reader(reader)
        {
        }

        bool isOpen() { return true; }

        void open() {}

        void close() {}

        uint32_t read(uint8_t* buf, uint32_t len)
        {
            int l = _reader.read((char*)buf, static_cast<int>(len));
            if (l == 0)
            {
                throw TTransportException(TTransportException::END_OF_FILE,
                    "no more data to read after end-of-buffer");
            }
            return (uint32_t)l;
        }

    private:
        binary_reader& _reader;
    };

    class binary_writer_transport : public TVirtualTransport<binary_writer_transport>
    {
    public:
        binary_writer_transport(binary_writer& writer)
            : _writer(writer)
        {
        }

        bool isOpen() { return true; }

        void open() {}

        void close() {}

        void write(const uint8_t* buf, uint32_t len)
        {
            _writer.write((const char*)buf, static_cast<int>(len));
        }

    private:
        binary_writer& _writer;
    };

    #define DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(TName, TTag, TMethod) \
        inline uint32_t write_base(::apache::thrift::protocol::TProtocol* proto, const TName& val)\
        {\
            return proto->write##TMethod(val); \
        }\
        inline uint32_t read_base(::apache::thrift::protocol::TProtocol* proto, /*out*/ TName& val)\
        {\
            return proto->read##TMethod(val); \
        }

    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(bool, BOOL, Bool)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int8_t, I08, Byte)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int16_t, I16, I16)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int32_t, I32, I32)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int64_t, I64, I64)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(double, DOUBLE, Double)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(std::string, STRING, String)

    template<typename T>
    uint32_t marshall_base(::apache::thrift::protocol::TProtocol* oproto, const T& val);
    template<typename T>
    uint32_t unmarshall_base(::apache::thrift::protocol::TProtocol* iproto, T& val);

    template<typename T>
    inline uint32_t write_base(::apache::thrift::protocol::TProtocol* oprot, const std::vector<T>& val)
    {
        uint32_t xfer = oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(val.size()));
        for (auto iter = val.begin(); iter != val.end(); ++iter)
        {
            marshall_base(oprot, *iter);
        }
        xfer += oprot->writeListEnd();
        return xfer;
    }

    template <typename T>
    inline uint32_t read_base(::apache::thrift::protocol::TProtocol* iprot, std::vector<T>& val)
    {
        uint32_t xfer = 0;

        val.clear();
        uint32_t size;
        ::apache::thrift::protocol::TType element_type;
        xfer += iprot->readListBegin(element_type, size);
        val.resize(size);
        for (uint32_t i = 0; i != size; ++i)
        {
            xfer += unmarshall_base(iprot, val[i]);
        }
        xfer += iprot->readListEnd();

        return xfer;
    }

    template<typename T_KEY, typename T_VALUE>
    inline uint32_t write_base(::apache::thrift::protocol::TProtocol* oprot, const std::map<T_KEY, T_VALUE>& val)
    {
        uint32_t xfer = 0;

        xfer += oprot->writeMapBegin(::apache::thrift::protocol::T_STRUCT, ::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(val.size()));
        for (std::map<T_KEY, T_VALUE> ::const_iterator iter = val.begin(); iter != val.end(); ++iter)
        {
            xfer += marshall_base(oprot, iter->first);
            xfer += marshall_base(oprot, iter->second);
        }
        xfer += oprot->writeMapEnd();

        return xfer;
    }

    template<typename T_KEY, typename T_VALUE>
    inline uint32_t read_base(::apache::thrift::protocol::TProtocol* iprot, std::map<T_KEY, T_VALUE>& val)
    {
        int xfer = 0;

        uint32_t size;
        ::apache::thrift::protocol::TType ktype;
        ::apache::thrift::protocol::TType vtype;
        xfer += iprot->readMapBegin(ktype, vtype, size);
        for (uint32_t i = 0; i < size; ++i)
        {
            T_KEY mkey;
            xfer += unmarshall_base(iprot, mkey);
            T_VALUE& mval = val[mkey];
            xfer += unmarshall_base(iprot, mval);
        }
        xfer += iprot->readMapEnd();

        return xfer;
    }

    inline const char* to_string(const rpc_address& addr) { return addr.to_string(); }
    inline const char* to_string(const blob& blob) { return ""; }
    inline const char* to_string(const task_code& code) { return code.to_string(); }
    inline const char* to_string(const error_code& ec) { return ec.to_string(); }
    inline const char* to_string(const gpid& id) { return "x"; } // not implemented
    inline const char* to_string(const atom_int& id) { return "x"; } // not implemented

    template<typename T>
    class serialization_forwarder
    {
    private:
        template<typename C>
        static constexpr auto check_method(C*) ->
            typename std::is_same< decltype(std::declval<C>().write(std::declval< ::apache::thrift::protocol::TProtocol* >())), uint32_t >::type;

        template<typename>
        static constexpr std::false_type check_method(...);

        typedef decltype(check_method<T>(nullptr)) has_read_write_method;

        static uint32_t marshall_internal(::apache::thrift::protocol::TProtocol* oproto, const T& value, std::false_type)
        {
            return write_base(oproto, value);
        }

        static uint32_t marshall_internal(::apache::thrift::protocol::TProtocol* oproto, const T& value, std::true_type)
        {
            return value.write(oproto);
        }

        static uint32_t unmarshall_internal(::apache::thrift::protocol::TProtocol* iproto, T& value, std::false_type)
        {
            return read_base(iproto, value);
        }

        static uint32_t unmarshall_internal(::apache::thrift::protocol::TProtocol* iproto, T& value, std::true_type)
        {
            return value.read(iproto);
        }

    public:
        static uint32_t marshall(::apache::thrift::protocol::TProtocol* oproto, const T& value)
        {
            return marshall_internal(oproto, value, has_read_write_method());
        }

        static uint32_t unmarshall(::apache::thrift::protocol::TProtocol* iproto, T& value)
        {
            return unmarshall_internal(iproto, value, has_read_write_method());
        }

    };

    template<typename TName>
    inline uint32_t marshall_base(::apache::thrift::protocol::TProtocol* oproto, const TName& val)
    {
        return serialization_forwarder<TName>::marshall(oproto, val);
    }

    template<typename TName>
    inline uint32_t unmarshall_base(::apache::thrift::protocol::TProtocol* iproto, /*out*/ TName& val)
    {
        //well, we assume read/write are in coupled
        return serialization_forwarder<TName>::unmarshall(iproto, val);
    }

#define GET_THRIFT_TYPE_MACRO(cpp_type, thrift_type) \
    inline ::apache::thrift::protocol::TType get_thrift_type(const cpp_type&)\
    {\
        return ::apache::thrift::protocol::thrift_type;\
    }\

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

    template<typename T>
    inline ::apache::thrift::protocol::TType get_thrift_type(const std::vector<T>&)
    {
        return ::apache::thrift::protocol::T_LIST;
    }

    template<typename T>
    inline ::apache::thrift::protocol::TType get_thrift_type(const T&)
    {
        return ::apache::thrift::protocol::T_STRUCT;
    }

    class char_ptr
    {
    private:
        const char* ptr;
        int length;
    public:
        char_ptr(const char* p, int len) : ptr(p), length(len) {}
        std::size_t size() const { return length; }
        const char* data() const { return ptr; }
    };

    class blob_string
    {
    private:
        blob& _buffer;
    public:
        blob_string(blob& bb) : _buffer(bb) {}

        void clear()
        {
            _buffer.assign(std::shared_ptr<char>(nullptr), 0, 0);
        }
        void resize(std::size_t new_size)
        {
            std::shared_ptr<char> b(new char[new_size], std::default_delete<char[]>());
            _buffer.assign(b, 0, new_size);
        }
        void assign(const char* ptr, std::size_t size)
        {
            std::shared_ptr<char> b(new char[size], std::default_delete<char[]>());
            memcpy(b.get(), ptr, size);
            _buffer.assign(b, 0, size);
        }
        const char* data() const
        {
            return _buffer.data();
        }
        size_t size() const
        {
            return _buffer.length();
        }

        char& operator [](int pos)
        {
            return const_cast<char*>(_buffer.data())[pos];
        }
    };

    inline uint32_t rpc_address::read(apache::thrift::protocol::TProtocol *iprot)
    {
        return iprot->readI64(reinterpret_cast<int64_t&>(_addr.u.value));
    }

    inline uint32_t rpc_address::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        return oprot->writeI64((int64_t)_addr.u.value);
    }
    
    inline uint32_t atom_int::read(apache::thrift::protocol::TProtocol *iprot)
    {
        int v;
        auto r = iprot->readI32(v);
        _value.store(v);
        return r;
    }

    inline uint32_t atom_int::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        return oprot->writeI32(_value.load());
    }

    inline uint32_t gpid::read(apache::thrift::protocol::TProtocol *iprot)
    {
        return iprot->readI64(reinterpret_cast<int64_t&>(_value.value));
    }

    inline uint32_t gpid::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        return oprot->writeI64((int64_t)_value.value);
    }
    
    inline uint32_t task_code::read(apache::thrift::protocol::TProtocol *iprot)
    {
        std::string task_code_string;
        uint32_t xfer = iprot->readString(task_code_string);
        _internal_code = dsn_task_code_from_string(task_code_string.c_str(), TASK_CODE_INVALID);
        return xfer;
    }

    inline uint32_t task_code::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        //for optimization, it is dangerous if the oprot is not a binary proto
        apache::thrift::protocol::TBinaryProtocol* binary_proto = static_cast<apache::thrift::protocol::TBinaryProtocol*>(oprot);
        const char* name = to_string();
        return binary_proto->writeString<char_ptr>(char_ptr(name, strlen(name)));
    }

    inline uint32_t blob::read(apache::thrift::protocol::TProtocol *iprot)
    {
        //for optimization, it is dangerous if the oprot is not a binary proto
        apache::thrift::protocol::TBinaryProtocol* binary_proto = static_cast<apache::thrift::protocol::TBinaryProtocol*>(iprot);
        blob_string str(*this);
        return binary_proto->readString<blob_string>(str);
    }

    inline uint32_t blob::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        apache::thrift::protocol::TBinaryProtocol* binary_proto = static_cast<apache::thrift::protocol::TBinaryProtocol*>(oprot);
        return binary_proto->writeString<blob_string>(blob_string(const_cast<blob&>(*this)));
    }

    inline uint32_t error_code::read(apache::thrift::protocol::TProtocol *iprot)
    {
        std::string ec_string;
        uint32_t xfer = iprot->readString(ec_string);
        _internal_code = dsn_error_from_string(ec_string.c_str(), ERR_UNKNOWN);
        return xfer;
    }

    inline uint32_t error_code::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        //for optimization, it is dangerous if the oprot is not a binary proto
        apache::thrift::protocol::TBinaryProtocol* binary_proto = static_cast<apache::thrift::protocol::TBinaryProtocol*>(oprot);
        const char* name = to_string();
        return binary_proto->writeString<char_ptr>(char_ptr(name, strlen(name)));
    }

    DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_THRIFT)

    class thrift_binary_message_parser : public message_parser
    {
    public:
        static void register_parser()
        {
            ::dsn::tools::register_message_header_parser<thrift_binary_message_parser>(NET_HDR_THRIFT);
        }

    private:
        // only one concurrent write message for each parser, so
        char _write_buffer_for_header[512];

    public:
        thrift_binary_message_parser(int buffer_block_size, bool is_write_only)
            : message_parser(buffer_block_size, is_write_only)
        {
        }

        //fixme
        virtual int prepare_buffers_on_send(message_ex* msg, int offset, /*out*/ send_buf* buffers) override
        {
            return 0;
        }

        int prepare_buffers_on_send(message_ex* msg, /*out*/ std::vector<blob>& buffers)
        {
            // prepare head
            blob bb(_write_buffer_for_header, 0, 512);
            binary_writer writer(bb);
            ::dsn::binary_writer_transport trans(writer);
            boost::shared_ptr< ::dsn::binary_writer_transport> transport(&trans, [](::dsn::binary_writer_transport*) {});
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);

            // FIXME:
            auto sp = task_spec::get(msg->header->rpc_id);

            proto.writeMessageBegin(msg->header->rpc_name,
                sp->type == TASK_TYPE_RPC_REQUEST ?
                ::apache::thrift::protocol::T_CALL :
                ::apache::thrift::protocol::T_REPLY,
                (int32_t)msg->header->id
                );

            // patched end (writeMessageEnd)
            // no need for now as no data is written

            // finalize
            std::vector<blob> lbuffers;
            //FIXME:
            //msg->writer().get_buffers(lbuffers);
            if (lbuffers[0].length() == sizeof(message_header))
            {
                lbuffers[0] = writer.get_buffer();
                buffers = lbuffers;
            }
            else
            {
                dassert(lbuffers[0].length() > sizeof(message_header), "");
                buffers.resize(lbuffers.size() + 1);
                buffers[0] = writer.get_buffer();

                for (int i = 0; i < static_cast<int>(lbuffers.size()); i++)
                {
                    if (i == 0)
                    {
                        buffers[1] = lbuffers[0].range(sizeof(message_header));
                    }
                    else
                    {
                        buffers[i + 1] = lbuffers[i];
                    }
                }
            }

            //FIXME:
            return 0;
        }

        virtual int get_send_buffers_count(message_ex *msg)
        {
            return 0;
        }

        virtual message_ex* get_message_on_receive(int read_length, /*out*/ int& read_next) override
        {
            mark_read(read_length);

            if (_read_buffer_occupied < 10)
            {
                read_next = 128;
                return nullptr;
            }

            try
            {
                blob bb = _read_buffer.range(0, _read_buffer_occupied);
                binary_reader reader(bb);
                ::dsn::binary_reader_transport trans(reader);
                boost::shared_ptr< ::dsn::binary_reader_transport> transport(&trans, [](::dsn::binary_reader_transport*) {});
                ::apache::thrift::protocol::TBinaryProtocol proto(transport);

                int32_t rseqid = 0;
                std::string fname;
                ::apache::thrift::protocol::TMessageType mtype;

                proto.readMessageBegin(fname, mtype, rseqid);
                int hdr_sz = _read_buffer_occupied - reader.get_remaining_size();

                if (mtype == ::apache::thrift::protocol::T_EXCEPTION)
                {
                    proto.skip(::apache::thrift::protocol::T_STRUCT);
                }
                else
                {
                    proto.skip(::apache::thrift::protocol::T_STRUCT);
                }

                proto.readMessageEnd();
                proto.getTransport()->readEnd();

                // msg done
                int msg_sz = _read_buffer_occupied - reader.get_remaining_size() - hdr_sz;
                auto msg_bb = _read_buffer.range(hdr_sz, msg_sz);
                message_ex* msg = message_ex::create_receive_message(msg_bb);
                msg->header->id = msg->header->rpc_id = rseqid;
                strncpy(msg->header->rpc_name, fname.c_str(), sizeof(msg->header->rpc_name));
                msg->header->body_length = msg_sz;

                _read_buffer = _read_buffer.range(msg_sz + hdr_sz);
                _read_buffer_occupied -= (msg_sz + hdr_sz);
                read_next = 128;
                return msg;
            }
            catch (TTransportException& ex)
            {
                ex;
                return nullptr;
            }
        }
    };

    template<typename T>
    inline void marshall_thrift_internal(const T &val, ::apache::thrift::protocol::TProtocol *proto)
    {
        /* 
           Here we must invoke writeStructBegin. 
           Although it does nothing when protocol is binary,
           json protocol indeed need it to generate a complete json object.
        */
        proto->writeStructBegin("args");
        proto->writeFieldBegin("value", get_thrift_type(val), 0);
        marshall_base<T>(proto, val);
        proto->writeFieldEnd();
        proto->writeFieldStop();
        proto->writeStructEnd();
    }

    template<typename T>
    inline void marshall_thrift_binary(binary_writer& writer, const T& val)
    {
        ::dsn::binary_writer_transport trans(writer);
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(&trans, [](::dsn::binary_writer_transport*) {});
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        marshall_thrift_internal(val, &proto);
        proto.getTransport()->flush();
    }

    template<typename T>
    inline void marshall_thrift_json(binary_writer& writer, const T& val)
    {
        ::dsn::binary_writer_transport trans(writer);
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(&trans, [](::dsn::binary_writer_transport*) {});
        ::apache::thrift::protocol::TJSONProtocol proto(transport);
        marshall_thrift_internal(val, &proto);
        proto.getTransport()->flush();
    }

    template<typename T>
    inline void unmarshall_thrift_internal(T &val, ::apache::thrift::protocol::TProtocol *proto)
    {
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;
        proto->readStructBegin(fname);
        while (true)
        {
            proto->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP)
            {
                break;
            }
            switch (fid)
            {
            case 0:
                if (ftype == get_thrift_type(val))
                {
                    unmarshall_base<T>(proto, val);
                }
                else
                {
                    proto->skip(ftype);
                }
                break;
            default:
                proto->skip(ftype);
                break;
            }
            proto->readFieldEnd();
        }
        proto->readStructEnd();
    }

    template<typename T>
    inline void unmarshall_thrift_binary(binary_reader& reader, T &val)
    {
        ::dsn::binary_reader_transport trans(reader);
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(&trans, [](::dsn::binary_reader_transport*) {});
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        unmarshall_thrift_internal(val, &proto);
    }

    template<typename T>
    inline void unmarshall_thrift_json(binary_reader& reader, T &val)
    {
        ::dsn::binary_reader_transport trans(reader);
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(&trans, [](::dsn::binary_reader_transport*) {});
        ::apache::thrift::protocol::TJSONProtocol proto(transport);
        unmarshall_thrift_internal(val, &proto);
    }
}
