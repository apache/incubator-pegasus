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
#include <stdint.h>
#include <atomic>
#include <vector>

#include "common/gpid.h"
#include "rpc_address.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/extensible_object.h"
#include "utils/link.h"

#define DSN_MAX_TASK_CODE_NAME_LENGTH 48
#define DSN_MAX_ERROR_CODE_NAME_LENGTH 48

namespace dsn {
class rpc_session;

typedef dsn::ref_ptr<rpc_session> rpc_session_ptr;

struct fast_code
{
    uint32_t local_code;

    // same hash from two processes indicates that
    // the mapping of rpc string and id are consistent, which
    // we leverage for optimization (fast rpc handler lookup)
    uint32_t local_hash;
};

typedef union msg_context
{
    struct
    {
        uint64_t is_request : 1;           ///< whether the RPC message is a request or response
        uint64_t is_forwarded : 1;         ///< whether the msg is forwarded or not
        uint64_t unused : 4;               ///< not used yet
        uint64_t serialize_format : 4;     ///< dsn_msg_serialize_format
        uint64_t is_forward_supported : 1; ///< whether support forwarding a message to real leader
        uint64_t is_backup_request : 1;    ///< whether the RPC is a backup request
        uint64_t reserved : 52;
    } u;
    uint64_t context; ///< msg_context is of sizeof(uint64_t)
} msg_context_t;

typedef struct message_header
{
    // For thrift protocol this is "THFT".
    // For dsn protocol this is "RDSN".
    // For http protocol this is either a "GET " or "POST".
    uint32_t hdr_type;

    uint32_t hdr_version;
    uint32_t hdr_length;
    uint32_t hdr_crc32;
    uint32_t body_length;
    uint32_t body_crc32;
    uint64_t id;       // sequence id, used to match request and response
    uint64_t trace_id; // used for tracking source
    char rpc_name[DSN_MAX_TASK_CODE_NAME_LENGTH];
    fast_code rpc_code; // dsn::task_code
    dsn::gpid gpid;     // global partition id
    msg_context_t context;

    // Attention:
    // here, from_address must be IPv4 address, namely we can regard from_address as a
    // POD-type structure, so no memory-leak will occur even if we don't call it's
    // destructor.
    //
    // generally, it is the from_node's primary address, except the
    // case described in message_ex::create_response()'s ATTENTION comment.
    //
    // in the forwarding case, the from_address is always the orignal client's address
    rpc_address from_address;

    struct
    {
        int32_t timeout_ms;      // rpc timeout in milliseconds
        int32_t thread_hash;     // thread hash used for thread dispatching
        uint64_t partition_hash; // partition hash used for calculating partition index
    } client;

    struct
    {
        char error_name[DSN_MAX_ERROR_CODE_NAME_LENGTH];
        fast_code error_code; // dsn::error_code
    } server;

    message_header() = default;
    ~message_header() = default;
} message_header;

class message_ex : public ref_counter, public extensible_object<message_ex, 4>
{
public:
    message_header *header;
    // "buffers" are used to manage memory allocated for this message.
    // the memory used by "header" is also mamanged in "buffers".
    //
    // please see "create_request", "create_recieve_message",
    // "create_receive_message_with_standalone_header" for the details on
    // how the headers managed by buffer
    std::vector<blob> buffers;

    // by rpc and network
    rpc_session_ptr io_session; // send/recv session
    rpc_address to_address;     // always ipv4/v6 address, it is the to_node's net address
    rpc_address server_address; // used by requests, and may be of uri/group address
    dsn::task_code local_rpc_code;
    network_header_format hdr_format;
    int send_retry_count;

    // by message queuing
    dlink dl;

public:
    // message_ex(blob bb, bool parse_hdr = true); // read
    ~message_ex();

    //
    // utility routines
    //
    error_code error();
    task_code rpc_code();
    static uint64_t new_id() { return ++_id; }
    static unsigned int get_body_length(char *hdr) { return ((message_header *)hdr)->body_length; }

    //
    // routines for create messages
    //
    static message_ex *create_receive_message(const blob &data);
    static message_ex *create_request(dsn::task_code rpc_code,
                                      int timeout_milliseconds = 0,
                                      int thread_hash = 0,
                                      uint64_t partition_hash = 0);

    static message_ex *create_received_request(dsn::task_code rpc_code,
                                               dsn_msg_serialize_format format,
                                               void *buffer,
                                               int size,
                                               int thread_hash = 0,
                                               uint64_t partition_hash = 0);

    /// This method is only used for receiving request.
    /// The returned message:
    ///   - msg->buffers[0] = message_header
    ///   - msg->buffers[1] = data
    /// NOTE: the reference counter of returned message_ex is not added in this function
    static message_ex *create_receive_message_with_standalone_header(const blob &data);

    /// copy message without client information, it will not reply
    /// The returned message:
    ///   - msg->buffers[0] = message_header
    ///   - msg->buffers[1] = data
    static message_ex *copy_message_no_reply(const message_ex &old_msg);

    /// The returned message:
    ///   - msg->buffers[0] = message_header
    ///   - msg->_is_read = false
    ///   - msg->_rw_index = 0
    ///   - msg->_rw_offset = 48 (size of message_header)
    message_ex *create_response();

    message_ex *copy(bool clone_content, bool copy_for_receive);
    message_ex *copy_and_prepare_send(bool clone_content);

    //
    // routines for buffer management
    //
    void write_next(void **ptr, size_t *size, size_t min_size);
    void write_commit(size_t size);
    bool read_next(void **ptr, size_t *size);
    bool read_next(blob &data);
    void read_commit(size_t size);
    size_t body_size() { return (size_t)header->body_length; }
    void *rw_ptr(size_t offset_begin);

    // rpc_read_stream can read a msg many times by restore()
    // rpc_read_stream stream1(msg)
    // msg->restore_read()
    // rpc_read_stream stream2(msg)
    void restore_read();

    bool is_backup_request() const { return header->context.u.is_backup_request; }

private:
    message_ex();
    void prepare_buffer_header();
    void release_buffer_header();

private:
    static std::atomic<uint64_t> _id;

private:
    // by msg read & write
    int _rw_index;      // current buffer index
    int _rw_offset;     // current buffer offset
    bool _rw_committed; // mark if it is in middle state of reading/writing
    bool _is_read;      // is for read(recv) or write(send)
};
typedef dsn::ref_ptr<message_ex> message_ptr;

} // namespace dsn
