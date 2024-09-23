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

#include "raw_message_parser.h"

#include <string.h>
#include <atomic>
#include <vector>

#include "common/gpid.h"
#include "network.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_message.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "utils/blob.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/threadpool_code.h"

namespace dsn {

DEFINE_TASK_CODE_RPC(RPC_CALL_RAW_SESSION_DISCONNECT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_RPC(RPC_CALL_RAW_MESSAGE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

// static
void raw_message_parser::notify_rpc_session_disconnected(rpc_session *sp)
{
    if (!sp->is_client()) {
        message_ex *special_msg = message_ex::create_receive_message_with_standalone_header(blob());
        dsn::message_header *header = special_msg->header;
        header->context.u.is_request = 1;
        header->context.u.is_forwarded = 0;
        header->from_address = sp->remote_address();
        header->gpid.set_value(0);

        strncpy(header->rpc_name, "RPC_CALL_RAW_SESSION_DISCONNECT", sizeof(header->rpc_name) - 1);
        header->rpc_name[sizeof(header->rpc_name) - 1] = '\0';
        special_msg->local_rpc_code = RPC_CALL_RAW_SESSION_DISCONNECT;
        special_msg->hdr_format = NET_HDR_RAW;
        sp->on_recv_message(special_msg, 0);
    }
}

raw_message_parser::raw_message_parser()
{
    bool hooked = false;
    static std::atomic_bool s_handler_hooked(false);
    if (s_handler_hooked.compare_exchange_strong(hooked, true)) {
        LOG_INFO("join point on_rpc_session_disconnected registered to notify disconnect with "
                 "RPC_CALL_RAW_SESSION_DISCONNECT");
        rpc_session::on_rpc_session_disconnected.put_back(
            raw_message_parser::notify_rpc_session_disconnected,
            "notify disconnect with RPC_CALL_RAW_SESSION_DISCONNECT");
    }
}

message_ex *raw_message_parser::get_message_on_receive(message_reader *reader,
                                                       /*out*/ int &read_next)
{
    if (reader->_buffer_occupied == 0) {
        if (reader->_buffer.length() > 0)
            read_next = reader->_buffer.length();
        else
            read_next = reader->_buffer_block_size;
        return nullptr;
    } else {
        auto msg_length = reader->_buffer_occupied;
        dsn::blob msg_blob = reader->_buffer.range(0, msg_length);
        message_ex *new_message =
            message_ex::create_receive_message_with_standalone_header(msg_blob);
        message_header *header = new_message->header;

        header->hdr_length = sizeof(*header);
        header->body_length = msg_length;
        strncpy(header->rpc_name, "RPC_CALL_RAW_MESSAGE", sizeof(header->rpc_name) - 1);
        header->rpc_name[sizeof(header->rpc_name) - 1] = '\0';
        header->gpid.set_value(0);
        header->context.u.is_request = 1;
        header->context.u.is_forwarded = 0;
        header->context.u.is_forward_supported = 0;

        reader->_buffer = reader->_buffer.range(msg_length);
        reader->_buffer_occupied = 0;
        read_next = 0;

        new_message->local_rpc_code = RPC_CALL_RAW_MESSAGE;
        new_message->hdr_format = NET_HDR_RAW;
        return new_message;
    }
}

int raw_message_parser::get_buffers_on_send(message_ex *msg, send_buf *buffers)
{
    // we must skip the message header
    unsigned int offset = sizeof(message_header);
    int i = 0;
    for (blob &buf : msg->buffers) {
        if (offset >= buf.length()) {
            offset -= buf.length();
            continue;
        }
        buffers[i].buf = (void *)(buf.data() + offset);
        buffers[i].sz = buf.length() - offset;
        offset = 0;
        ++i;
    }
    return i;
}
} // namespace dsn
