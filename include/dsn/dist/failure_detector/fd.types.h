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
# pragma once
# include <dsn/internal/serialization.h>

namespace dsn { namespace fd { 
    // ---------- beacon_msg -------------
    struct beacon_msg
    {
        int64_t time;
        ::dsn::end_point from;
        ::dsn::end_point to;
    };

    inline void marshall(::dsn::binary_writer& writer, const beacon_msg& val)
    {
        marshall(writer, val.time);
        marshall(writer, val.from);
        marshall(writer, val.to);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param beacon_msg& val)
    {
        unmarshall(reader, val.time);
        unmarshall(reader, val.from);
        unmarshall(reader, val.to);
    };

    // ---------- beacon_ack -------------
    struct beacon_ack
    {
        int64_t time;
        ::dsn::end_point this_node;
        ::dsn::end_point primary_node;
        bool is_master;
        bool allowed;
    };

    inline void marshall(::dsn::binary_writer& writer, const beacon_ack& val)
    {
        marshall(writer, val.time);
        marshall(writer, val.this_node);
        marshall(writer, val.primary_node);
        marshall(writer, val.is_master);
        marshall(writer, val.allowed);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param beacon_ack& val)
    {
        unmarshall(reader, val.time);
        unmarshall(reader, val.this_node);
        unmarshall(reader, val.primary_node);
        unmarshall(reader, val.is_master);
        unmarshall(reader, val.allowed);
    };

} } 
