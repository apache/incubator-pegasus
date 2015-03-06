# pragma once

# include <rdsn/internal/rpc_message.h>
# include <list>
# include <map>
# include <set>
# include <vector>

// pod types
#define DEFINE_POD_SERIALIZATION(T) \
    inline void marshall(::rdsn::utils::binary_writer& writer, const T& val, uint16_t pos = 0xffff)\
    {\
    writer.write((const char*)&val, (int)sizeof(val), pos); \
    }\
    inline void unmarshall(::rdsn::utils::binary_reader& reader, __out T& val)\
    {\
    reader.read((char*)&val, (int)sizeof(T)); \
    }

namespace rdsn {
    
    template<typename T>
    inline void marshall(::rdsn::message_ptr& writer, const T& val, uint16_t pos = 0xffff)
    {
        marshall(writer->writer(), val, pos);
    }

    template<typename T>
    inline void unmarshall(::rdsn::message_ptr& reader, __out T& val)
    {
        unmarshall(reader->reader(), val);
    }

    namespace utils {

#ifndef ZION_NOT_USE_DEFAULT_SERIALIZATION

        DEFINE_POD_SERIALIZATION(bool)
            DEFINE_POD_SERIALIZATION(char)
            //DEFINE_POD_SERIALIZATION(size_t)
            DEFINE_POD_SERIALIZATION(float)
            DEFINE_POD_SERIALIZATION(double)
            DEFINE_POD_SERIALIZATION(int8_t)
            DEFINE_POD_SERIALIZATION(uint8_t)
            DEFINE_POD_SERIALIZATION(int16_t)
            DEFINE_POD_SERIALIZATION(uint16_t)
            DEFINE_POD_SERIALIZATION(int32_t)
            DEFINE_POD_SERIALIZATION(uint32_t)
            DEFINE_POD_SERIALIZATION(int64_t)
            DEFINE_POD_SERIALIZATION(uint64_t)

            // error_code
            inline void marshall(::rdsn::utils::binary_writer& writer, const error_code& val, uint16_t pos = 0xffff)
        {
            int err = val.get();
            marshall(writer, err, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out error_code& val)
        {
            int err;
            unmarshall(reader, err);
            val.set(err);
        }


        // std::string
        inline void marshall(::rdsn::utils::binary_writer& writer, const std::string& val, uint16_t pos = 0xffff)
        {
            writer.write(val, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out std::string& val)
        {
            reader.read(val);
        }

        // end point
        //extern inline void marshall(::rdsn::utils::binary_writer& writer, const end_point& val, uint16_t pos = 0xffff);
        //extern inline void unmarshall(::rdsn::utils::binary_reader& reader, __out end_point& val);

        // blob
        inline void marshall(::rdsn::utils::binary_writer& writer, const utils::blob& val, uint16_t pos = 0xffff)
        {
            writer.write(val, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out utils::blob& val)
        {
            reader.read(val);
        }

        // for generic list
        template<typename T>
        inline void marshall(::rdsn::utils::binary_writer& writer, const std::list<T>& val, uint16_t pos = 0xffff)
        {
            int sz = val.size();
            marshall(writer, sz, pos);
            for (auto& v : val)
            {
                marshall(writer, v, pos);
            }
        }

        template<typename T>
        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out std::list<T>& val)
        {
            int sz;
            unmarshall(reader, sz);
            val.resize(sz);
            for (auto& v : val)
            {
                unmarshall(reader, v);
            }
        }

        // for generic vector
        template<typename T>
        inline void marshall(::rdsn::utils::binary_writer& writer, const std::vector<T>& val, uint16_t pos = 0xffff)
        {
            int sz = val.size();
            marshall(writer, sz, pos);
            for (auto& v : val)
            {
                marshall(writer, v, pos);
            }
        }

        template<typename T>
        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out std::vector<T>& val)
        {
            int sz;
            unmarshall(reader, sz);
            val.resize(sz);
            for (auto& v : val)
            {
                unmarshall(reader, v);
            }
        }

        // for generic set
        template<typename T>
        inline void marshall(::rdsn::utils::binary_writer& writer, const std::set<T, std::less<T>, std::allocator<T>>& val, uint16_t pos = 0xffff)
        {
            int sz = val.size();
            marshall(writer, sz, pos);
            for (auto& v : val)
            {
                marshall(writer, v, pos);
            }
        }

        template<typename T>
        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out std::set<T, std::less<T>, std::allocator<T>>& val)
        {
            int sz;
            unmarshall(reader, sz);
            val.resize(sz);
            for (auto& v : val)
            {
                unmarshall(reader, v);
            }
        }
#endif
    }
}