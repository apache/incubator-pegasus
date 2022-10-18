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
 *     message parser manager
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "runtime/rpc/message_parser.h"

namespace dsn {
class message_parser_manager : public utils::singleton<message_parser_manager>
{
public:
    struct parser_factory_info
    {
        parser_factory_info() : fmt(NET_HDR_INVALID), factory(nullptr), parser_size(0) {}

        network_header_format fmt;
        message_parser::factory factory;
        size_t parser_size;
    };

public:
    // called only during system init, thread-unsafe
    void register_factory(network_header_format fmt,
                          const std::vector<const char *> &signatures,
                          message_parser::factory f,
                          size_t sz);

    message_parser *create_parser(network_header_format fmt);
    const parser_factory_info &get(network_header_format fmt) { return _factory_vec[fmt]; }

private:
    friend class utils::singleton<message_parser_manager>;
    message_parser_manager() = default;
    ~message_parser_manager() = default;

    std::vector<parser_factory_info> _factory_vec;
};
}
