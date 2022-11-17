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

#include "message_parser_manager.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"

namespace dsn {

// ------------------- header type ------------------------------
struct header_type
{
public:
    union
    {
        char stype[4];
        int32_t itype;
    } type;

    header_type() { type.itype = -1; }

    header_type(int32_t itype) { type.itype = itype; }

    header_type(const char *str) { memcpy(type.stype, str, sizeof(int32_t)); }

    header_type(const header_type &another) { type.itype = another.type.itype; }

    header_type &operator=(const header_type &another)
    {
        type.itype = another.type.itype;
        return *this;
    }

    bool operator==(const header_type &other) const { return type.itype == other.type.itype; }

    bool operator!=(const header_type &other) const { return type.itype != other.type.itype; }

    std::string debug_string() const;

public:
    static network_header_format header_type_to_c_type(const header_type &hdr_type);
    static void register_header_signature(int32_t sig, network_header_format type);

private:
    static std::unordered_map<int32_t, network_header_format> s_fmt_map;
};

std::unordered_map<int32_t, network_header_format> header_type::s_fmt_map;

std::string header_type::debug_string() const
{
    char buf[20];
    char *ptr = buf;
    for (int i = 0; i < 4; ++i) {
        auto &c = type.stype[i];
        if (isprint(c)) {
            *ptr++ = c;
        } else {
            sprintf(ptr, "\\%02X", c);
            ptr += 3;
        }
    }
    *ptr = '\0';
    return std::string(buf);
}

/*static*/ network_header_format header_type::header_type_to_c_type(const header_type &hdr_type)
{
    auto it = s_fmt_map.find(hdr_type.type.itype);
    if (it != s_fmt_map.end()) {
        return it->second;
    } else
        return NET_HDR_INVALID;
}

/*static*/ void header_type::register_header_signature(int32_t sig, network_header_format type)
{
    auto it = s_fmt_map.find(sig);
    if (it != s_fmt_map.end()) {
        CHECK_EQ_MSG(it->second,
                     type,
                     "signature {:#010x} is already registerd for header type {}",
                     sig,
                     type);
    } else {
        s_fmt_map.emplace(sig, type);
    }
}

/*static*/ network_header_format message_parser::get_header_type(const char *bytes)
{
    header_type ht(bytes);
    return header_type::header_type_to_c_type(ht);
}

/*static*/ std::string message_parser::get_debug_string(const char *bytes)
{
    header_type ht(bytes);
    return ht.debug_string();
}

//-------------------- msg reader --------------------
char *message_reader::read_buffer_ptr(unsigned int read_next)
{
    if (read_next + _buffer_occupied > _buffer.length()) {
        // remember currently read content
        blob rb;
        if (_buffer_occupied > 0)
            rb = _buffer.range(0, _buffer_occupied);

        // switch to next
        unsigned int sz =
            (read_next + _buffer_occupied > _buffer_block_size ? read_next + _buffer_occupied
                                                               : _buffer_block_size);
        // TODO(wutao1): make it a buffer queue like what sofa-pbrpc does
        //               (https://github.com/baidu/sofa-pbrpc/blob/master/src/sofa/pbrpc/buffer.h)
        //               to reduce memory copy.
        _buffer.assign(dsn::utils::make_shared_array<char>(sz), 0, sz);
        _buffer_occupied = 0;

        // copy
        if (rb.length() > 0) {
            // every read buffer_block_size data may cause one copy
            memcpy((void *)_buffer.data(), (const void *)rb.data(), rb.length());
            _buffer_occupied = rb.length();
        }

        CHECK_LE_MSG(read_next + _buffer_occupied,
                     _buffer.length(),
                     "read_next: {}, _buffer_occupied: {}",
                     read_next,
                     _buffer_occupied);
    }

    return (char *)(_buffer.data() + _buffer_occupied);
}

//-------------------- msg parser manager --------------------
void message_parser_manager::register_factory(network_header_format fmt,
                                              const std::vector<const char *> &signatures,
                                              message_parser::factory f,
                                              size_t sz)
{
    if (static_cast<unsigned int>(fmt) >= _factory_vec.size()) {
        _factory_vec.resize(fmt + 1);
    }

    parser_factory_info &info = _factory_vec[fmt];
    info.fmt = fmt;
    info.factory = f;
    info.parser_size = sz;

    for (auto &v : signatures) {
        header_type type(v);
        header_type::register_header_signature(type.type.itype, fmt);
    }
}

message_parser *message_parser_manager::create_parser(network_header_format fmt)
{
    parser_factory_info &info = _factory_vec[fmt];
    if (info.factory)
        return info.factory();
    else
        return nullptr;
}
}
