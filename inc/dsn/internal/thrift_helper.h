/*
* The MIT License (MIT)

* Copyright (c) 2015 Microsoft Corporation

* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:

* The above copyright notice and this permission notice shall be included in
* all copies or substantial portions of the Software.

* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
* THE SOFTWARE.
*/

# pragma once

# define ZION_NOT_USE_DEFAULT_SERIALIZATION 1
# include <dsn/internal/serialization.h>
# include <dsn/internal/utils.h>

# include <thrift/Thrift.h>
# include <thrift/protocol/TBinaryProtocol.h>
# include <thrift/protocol/TVirtualProtocol.h>
# include <thrift/transport/TVirtualTransport.h>

using namespace ::apache::thrift::transport;

namespace dsn {
    namespace utils {

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
                _reader.read((char*)buf, (int)len);
                return len;
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
                _writer.write((const char*)buf, (int)len);
            }

        private:
            binary_writer& _writer;
        };


        /*
        TYPE_VOID,
        TYPE_STRING,
        TYPE_BOOL,
        TYPE_BYTE,
        TYPE_I16,
        TYPE_I32,
        TYPE_I64,
        TYPE_DOUBLE
        */

    #define DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(TName, TTag, TMethod) \
        inline void marshall(::dsn::utils::binary_writer& writer, const TName& val)\
        {\
            boost::shared_ptr<::dsn::utils::binary_writer_transport> transport(new ::dsn::utils::binary_writer_transport(writer));\
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);\
            uint32_t xfer = 0;\
            \
            xfer += proto.writeStructBegin("val");\
            xfer += proto.writeFieldBegin("val", ::apache::thrift::protocol::T_##TTag, 0); \
            xfer += proto.write##TMethod(val); \
            xfer += proto.writeFieldEnd(); \
            xfer += proto.writeFieldStop();\
            xfer += proto.writeStructEnd();\
        }\
        \
        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param TName& val)\
        {\
            boost::shared_ptr<::dsn::utils::binary_reader_transport> transport(new ::dsn::utils::binary_reader_transport(reader));\
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);\
            uint32_t xfer = 0;\
            std::string fname;\
            ::apache::thrift::protocol::TType ftype;\
            int16_t fid;\
            \
            xfer += proto.readStructBegin(fname);\
            \
            using ::apache::thrift::protocol::TProtocolException;\
            \
            while (true)\
            {\
                xfer += proto.readFieldBegin(fname, ftype, fid);\
                if (ftype == ::apache::thrift::protocol::T_STOP) {\
                    break;\
                }\
                switch (fid)\
                {\
                case 0:\
                if (ftype == ::apache::thrift::protocol::T_##TTag) {\
                    xfer += proto.read##TMethod(val); \
                    }\
                    else {\
                        xfer += proto.skip(ftype);\
                    }\
                    break;\
                default:\
                    xfer += proto.skip(ftype);\
                    break;\
                }\
                xfer += proto.readFieldEnd();\
            }\
            \
            xfer += proto.readStructEnd();\
        }

        DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(bool, BOOL, Bool)
        DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int8_t, BYTE, Byte)
        DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int16_t, I16, I16)
        DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int32_t, I32, I32)
        DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int64_t, I64, I64)

        DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(double, DOUBLE, Double)
        
        DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(std::string, STRING, String)

    }
}

namespace apache {
    namespace thrift {
        namespace transport {
            inline const char* TTransportException::what() const throw() {
                if (message_.empty()) {
                    switch (type_) {
                    case UNKNOWN:
                        return "TTransportException: Unknown transport exception";
                    case NOT_OPEN:
                        return "TTransportException: Transport not open";
                    case TIMED_OUT:
                        return "TTransportException: Timed out";
                    case END_OF_FILE:
                        return "TTransportException: End of file";
                    case INTERRUPTED:
                        return "TTransportException: Interrupted";
                    case BAD_ARGS:
                        return "TTransportException: Invalid arguments";
                    case CORRUPTED_DATA:
                        return "TTransportException: Corrupted Data";
                    case INTERNAL_ERROR:
                        return "TTransportException: Internal error";
                    default:
                        return "TTransportException: (Invalid exception type)";
                    }
                }
                else {
                    return message_.c_str();
                }
            }
        }
    }
} // apache::thrift::transport
