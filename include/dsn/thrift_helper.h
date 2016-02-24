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
 */

# pragma once

# include <dsn/internal/message_parser.h>
# include <dsn/tool_api.h>

# include <thrift/Thrift.h>
# include <thrift/protocol/TBinaryProtocol.h>
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
    inline uint32_t write_base(::apache::thrift::protocol::TProtocol* proto, const T& value)
    {
        switch (sizeof(value))
        {
        case 1:
            return write_base(proto, (int8_t)value);
        case 2:
            return write_base(proto, (int16_t)value);
        case 4:
            return write_base(proto, (int32_t)value);
        case 8:
            return write_base(proto, (int64_t)value);
        default:
            assert(false);
            return 0;
        }
    }

    template <typename T>
    inline uint32_t read_base(::apache::thrift::protocol::TProtocol* proto, T& value)
    {
        uint32_t res = 0;
        switch (sizeof(value))
        {
        case 1: {
            int8_t val;
            res = read_base(proto, val);
            value = (T)val;
            return res;
        }
        case 2: {
            int16_t val;
            res = read_base(proto, val);
            value = (T)val;
            return res;
        }
        case 4: {
            int32_t val;
            res = read_base(proto, val);
            value = T(val);
            return res;
        }
        case 8: {
            int64_t val;
            res = read_base(proto, val);
            value = T(val);
            return res;
        }
        default:
            assert(false);
            return 0;
        }
    }

    template<typename T>
    inline uint32_t write_base(::apache::thrift::protocol::TProtocol* oprot, const std::vector<T>& val)
    {
        uint32_t xfer = oprot->writeFieldBegin("vector", ::apache::thrift::protocol::T_LIST, 1);
        xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(val.size()));
        for (auto iter = val.begin(); iter!=val.end(); ++iter)
        {
            marshall_base(oprot, *iter);
        }
        xfer += oprot->writeListEnd();
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeFieldEnd();
    }

    template <typename T>
    inline uint32_t read_base(::apache::thrift::protocol::TProtocol* iprot, std::vector<T>& val)
    {
        uint32_t xfer = 0;

        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        xfer += iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_LIST)
        {
            val.clear();
            uint32_t size;
            ::apache::thrift::protocol::TType element_type;
            xfer += iprot->readListBegin(element_type, size);
            val.resize(size);
            for (uint32_t i=0; i!=size; ++i)
            {
                xfer += unmarshall_base(iprot, val[i]);
            }
            xfer += iprot->readListEnd();
        }
        else
            xfer += iprot->skip(ftype);
        xfer += iprot->readFieldEnd();
        return xfer;
    }

    inline const char* to_string(const rpc_address& addr) { return ""; }
    inline const char* to_string(const blob& blob) { return ""; }
    inline const char* to_string(const task_code& code) { return ""; }
    inline const char* to_string(const error_code& ec) { return ""; }

    template<typename T>
    class serialization_forwarder
    {
    private:
        template<typename C>
        static constexpr auto check_method( C* ) ->
            typename std::is_same< decltype(std::declval<C>().write( std::declval< ::apache::thrift::protocol::TProtocol* >() ) ), uint32_t >::type;

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

    template<typename T>
    void marshall(binary_writer& writer, const T& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        marshall_base<T>(&proto, val);
    }

    template<typename T>
    void unmarshall(binary_reader& reader, /*out*/ T& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        unmarshall_base<T>(&proto, val);
    }

    inline uint32_t rpc_address::read(apache::thrift::protocol::TProtocol *iprot)
    {
        return iprot->readI64(reinterpret_cast<int64_t&>(_addr.u.value));
    }

    inline uint32_t rpc_address::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        return oprot->writeI64((int64_t)_addr.u.value);
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
        std::string str(to_string());
        return oprot->writeString(str);
    }

    inline uint32_t blob::read(apache::thrift::protocol::TProtocol *iprot)
    {
        std::string ptr;
        uint32_t xfer = iprot->readString(ptr);

        //TODO: need something to do to resolve the memory copy
        std::shared_ptr<char> buffer(new char [ptr.size()], std::default_delete<char[]>());
        memcpy(buffer.get(), ptr.c_str(), ptr.size());
        assign(buffer, 0, ptr.size());
        return xfer;
    }

    inline uint32_t blob::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        //TODO: need something to do to resolve the memory copy
        std::string ptr(_data, _length);
        return oprot->writeString(ptr);
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
        std::string ec_string(to_string());
        return oprot->writeString(ec_string);
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
            boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
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

        virtual int get_send_buffers_count_and_total_length(message_ex *msg, int *total_length)
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
                boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
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
}
