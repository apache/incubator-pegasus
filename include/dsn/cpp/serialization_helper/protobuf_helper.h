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
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/utility/ports.h>
# include <dsn/utility/utils.h>

# include <google/protobuf/stubs/common.h>
# include <google/protobuf/io/zero_copy_stream.h>
# include <google/protobuf/io/coded_stream.h>
# include <google/protobuf/util/json_util.h>
# include <google/protobuf/util/type_resolver_util.h>
# include <google/protobuf/io/zero_copy_stream_impl_lite.h>
# include <google/protobuf/descriptor.h>

namespace dsn {

    class gproto_binary_reader : public ::google::protobuf::io::ZeroCopyInputStream
    {
    public:
        gproto_binary_reader(binary_reader& reader)
            : _reader(reader)
        {
        }

        virtual bool Next(const void** data, int* size)
        {
            return _reader.next(data, size);
        }

        virtual void BackUp(int count)
        {
            _reader.backup(count);
        }

        virtual bool Skip(int count)
        {
            return _reader.skip(count);
        }

        virtual google::protobuf::int64 ByteCount() const
        {
            return static_cast<google::protobuf::int64>(_reader.total_size());
        }
            
    private:
        binary_reader& _reader;
    };

    class gproto_binary_writer : public ::google::protobuf::io::ZeroCopyOutputStream
    {
    public:
        gproto_binary_writer(binary_writer& writer)
            : _writer(writer)
        {
        }

        virtual bool Next(void** data, int* size)
        {
            return _writer.next(data, size);
        }

        virtual void BackUp(int count)
        {
            _writer.backup(count);
        }

        virtual google::protobuf::int64 ByteCount() const
        {
            return static_cast<google::protobuf::int64>(_writer.total_size());
        }

        virtual bool WriteAliasedRaw(const void* data, int size)
        {
            return false;
        }

    private:
        binary_writer& _writer;
    };

    template<typename T>
    void marshall_protobuf_binary(binary_writer& writer, const T& val)
    {
        gproto_binary_writer wt2(writer);
        ::google::protobuf::io::CodedOutputStream os(&wt2);
        val.SerializeToCodedStream(&os);
    }

    template<typename T>
    void unmarshall_protobuf_binary(binary_reader& reader, /*out*/ T& val)
    {
        gproto_binary_reader rd2(reader);
        ::google::protobuf::io::CodedInputStream is(&rd2);
        val.MergePartialFromCodedStream(&is);
    }

    /* you may want to specify your own url prefix here */
    static const char protobuf_kTypeUrlPrefix[] = "";

    static google::protobuf::util::TypeResolver *protobuf_type_resolver = google::protobuf::util::NewTypeResolverForDescriptorPool(
        protobuf_kTypeUrlPrefix, google::protobuf::DescriptorPool::generated_pool());
    
    template<typename T>
    inline std::string protobuf_resolve_type_url(const T& val)
    {
        return std::string(protobuf_kTypeUrlPrefix) + "/" + val.GetDescriptor()->full_name();
    }

    template<typename T>
    void marshall_protobuf_json(binary_writer& writer, const T& val)
    {
        std::string type_url = protobuf_resolve_type_url(val);
        std::string binary_str;
        val.SerializeToString(&binary_str);
        gproto_binary_writer wt2(writer);
        google::protobuf::io::ArrayInputStream input_stream(binary_str.data(), binary_str.size());
        google::protobuf::util::BinaryToJsonStream(protobuf_type_resolver, type_url, &input_stream, &wt2);
    }

    template<typename T>
    void unmarshall_protobuf_json(binary_reader& reader, /*out*/ T& val)
    {
        std::string type_url = protobuf_resolve_type_url(val);
        gproto_binary_reader rd2(reader);
        std::string binary_str;
        google::protobuf::io::StringOutputStream output_stream(&binary_str);
        google::protobuf::util::JsonToBinaryStream(protobuf_type_resolver, type_url, &rd2, &output_stream);
        val.ParseFromString(binary_str);
    }
}
