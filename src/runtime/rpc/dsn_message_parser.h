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

#include "runtime/rpc/message_parser.h"

namespace dsn {
class message_ex;

// Message parser for browser-generated http request.
class dsn_message_parser : public message_parser
{
public:
    dsn_message_parser() : _header_checked(false) {}
    virtual ~dsn_message_parser() {}

    virtual void reset() override;

    virtual message_ex *get_message_on_receive(message_reader *reader,
                                               /*out*/ int &read_next) override;

    virtual void prepare_on_send(message_ex *msg) override;

    virtual int get_buffers_on_send(message_ex *msg, /*out*/ send_buf *buffers) override;

private:
    static bool is_right_header(char *hdr);

    static bool is_right_body(message_ex *msg);

private:
    bool _header_checked;
};
}
