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

#include <stddef.h>
#include <string>
#include <vector>

#include "rpc/rpc_message.h"
#include "task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"

namespace dsn {

// TODO(wutao1): call it read_buffer, and make it an utility
// Not-Thread-Safe.
class message_reader
{
public:
    explicit message_reader(int buffer_block_size)
        : _buffer_occupied(0), _buffer_block_size(buffer_block_size)
    {
    }

    // called before read to extend read buffer
    char *read_buffer_ptr(unsigned int read_next);

    // get remaining buffer capacity
    unsigned int read_buffer_capacity() const { return _buffer.length() - _buffer_occupied; }

    // called after read to mark data occupied
    void mark_read(unsigned int read_length) { _buffer_occupied += read_length; }

    // discard read data
    void truncate_read() { _buffer_occupied = 0; }

    // mark the tailing `sz` of bytes are consumed and discardable.
    void consume_buffer(size_t sz)
    {
        _buffer = _buffer.range(sz);
        _buffer_occupied -= sz;
    }

    blob buffer() const { return _buffer.range(0, _buffer_occupied); }

public:
    // TODO(wutao1): make them private members
    blob _buffer;
    unsigned int _buffer_occupied;
    const unsigned int _buffer_block_size;
};

class message_parser;

typedef ref_ptr<message_parser> message_parser_ptr;

class message_parser : public ref_counter
{
public:
    template <typename T>
    static message_parser *create()
    {
        return new T();
    }

    typedef message_parser *(*factory)();

public:
    virtual ~message_parser() {}

    // reset the parser
    virtual void reset() {}

    // after read, see if we can compose a message
    // if read_next returns -1, indicated the the message is corrupted
    virtual message_ex *get_message_on_receive(message_reader *reader, /*out*/ int &read_next) = 0;

    // prepare buffer before send.
    // this method should be called before get_buffer_count_on_send() and get_buffers_on_send()
    // to do some prepare operation.
    // may be invoked for mutiple times if the message is reused for resending.
    virtual void prepare_on_send(message_ex *msg) {}

    struct send_buf
    {
        void *buf;
        size_t sz;
    };

    // get max buffer count needed by get_buffers_on_send().
    // may be invoked for mutiple times if the message is reused for resending.
    int get_buffer_count_on_send(message_ex *msg) const
    {
        return static_cast<int>(msg->buffers.size());
    }

    // get buffers from message to 'buffers'.
    // return buffer count used, which must be no more than the return value of
    // get_buffer_count_on_send().
    // may be invoked for mutiple times if the message is reused for resending.
    virtual int get_buffers_on_send(message_ex *msg, /*out*/ send_buf *buffers) = 0;

public:
    static network_header_format
    get_header_type(const char *bytes); // buffer size >= sizeof(uint32_t)
    static std::string get_debug_string(const char *bytes);
};

} // namespace dsn
