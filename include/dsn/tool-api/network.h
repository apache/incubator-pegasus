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
 *     base interface for a network provider
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/tool-api/task.h>
#include <dsn/utility/synchronize.h>
#include <dsn/tool-api/message_parser.h>
#include <dsn/tool-api/rpc_address.h>
#include <dsn/utility/exp_delay.h>
#include <dsn/utility/dlib.h>
#include <atomic>

namespace dsn {

class rpc_engine;
class service_node;
class task_worker_pool;
class task_queue;
/*!
@addtogroup tool-api-providers
@{
*/

/*!
  network bound to a specific rpc_channel and port (see start)
 !!! all threads must be started with task::set_tls_dsn_context(null, provider->node());
*/
class network
{
public:
    //
    // network factory prototype
    //
    template <typename T>
    static network *create(rpc_engine *srv, network *inner_provider)
    {
        return new T(srv, inner_provider);
    }

    typedef network *(*factory)(rpc_engine *, network *);

public:
    //
    // srv - the rpc engine, could contain many networks there
    // inner_provider - when not null, this network is simply a wrapper for tooling purpose (e.g.,
    // tracing)
    //                  all downcalls should be redirected to the inner provider in the end
    //
    DSN_API network(rpc_engine *srv, network *inner_provider);
    virtual ~network() {}

    //
    // when client_only is true, port is faked (equal to app id for tracing purpose)
    //
    virtual error_code start(rpc_channel channel, int port, bool client_only, io_modifer &ctx) = 0;

    //
    // the named address
    //
    virtual ::dsn::rpc_address address() = 0;

    //
    // this is where the upper rpc engine calls down for a RPC call
    //   request - the message to be sent, all meta info (e.g., timeout, server address are
    //             prepared ready in its header; use message_parser to extract
    //             blobs from message for sending
    //
    virtual void send_message(message_ex *request) = 0;

    //
    // tools in rDSN may decide to drop this msg,
    // in this case, the network should implement the appropriate
    // failure model that makes this failure possible in reality
    //
    virtual void inject_drop_message(message_ex *msg, bool is_send) = 0;

    //
    // utilities
    //
    DSN_API service_node *node() const;

    //
    // called when network received a complete request message
    //
    DSN_API void on_recv_request(message_ex *msg, int delay_ms);

    //
    // called when network received a complete reply message or network failed,
    // if network failed, the 'msg' will be nullptr
    //
    DSN_API void on_recv_reply(uint64_t id, message_ex *msg, int delay_ms);

    //
    // create a message parser for
    //  (1) extracing blob from a RPC request message for low layer'
    //  (2) parsing a incoming blob message to get the rpc_message
    //
    DSN_API message_parser *new_message_parser(network_header_format hdr_format);

    // for in-place new message parser
    DSN_API std::pair<message_parser::factory2, size_t>
    get_message_parser_info(network_header_format hdr_format);

    rpc_engine *engine() const { return _engine; }
    int max_buffer_block_count_per_send() const { return _max_buffer_block_count_per_send; }
    network_header_format client_hdr_format() const { return _client_hdr_format; }
    network_header_format unknown_msg_hdr_format() const { return _unknown_msg_header_format; }
    int message_buffer_block_size() const { return _message_buffer_block_size; }

protected:
    DSN_API static uint32_t get_local_ipv4();

protected:
    rpc_engine *_engine;
    network_header_format _client_hdr_format;
    network_header_format _unknown_msg_header_format; // default is NET_HDR_INVALID
    int _message_buffer_block_size;
    int _max_buffer_block_count_per_send;
    int _send_queue_threshold;

private:
    friend class rpc_engine;
    DSN_API void reset_parser_attr(network_header_format client_hdr_format,
                                   int message_buffer_block_size);
};

/*!
  an incomplete network implementation for connection oriented network, e.g., TCP
*/
class connection_oriented_network : public network
{
public:
    DSN_API connection_oriented_network(rpc_engine *srv, network *inner_provider);
    virtual ~connection_oriented_network() {}

    // server session management
    DSN_API rpc_session_ptr get_server_session(::dsn::rpc_address ep);
    DSN_API void on_server_session_accepted(rpc_session_ptr &s);
    DSN_API void on_server_session_disconnected(rpc_session_ptr &s);

    // client session management
    DSN_API rpc_session_ptr get_client_session(::dsn::rpc_address ep);
    DSN_API void on_client_session_connected(rpc_session_ptr &s);
    DSN_API void on_client_session_disconnected(rpc_session_ptr &s);

    // called upon RPC call, rpc client session is created on demand
    DSN_API virtual void send_message(message_ex *request) override;

    // called by rpc engine
    DSN_API virtual void inject_drop_message(message_ex *msg, bool is_send) override;

    // to be defined
    virtual rpc_session_ptr create_client_session(::dsn::rpc_address server_addr) = 0;

protected:
    typedef std::unordered_map<::dsn::rpc_address, rpc_session_ptr> client_sessions;
    client_sessions _clients; // to_address => rpc_session
    utils::rw_lock_nr _clients_lock;

    typedef std::unordered_map<::dsn::rpc_address, rpc_session_ptr> server_sessions;
    server_sessions _servers; // from_address => rpc_session
    utils::rw_lock_nr _servers_lock;
};

/*!
  session managements (both client and server types)
*/
class rpc_client_matcher;
class rpc_session : public ref_counter
{
public:
    /*!
    @addtogroup tool-api-hooks
    @{
    */
    DSN_API static join_point<void, rpc_session *> on_rpc_session_connected;
    DSN_API static join_point<void, rpc_session *> on_rpc_session_disconnected;
    /*@}*/
public:
    DSN_API rpc_session(connection_oriented_network &net,
                        ::dsn::rpc_address remote_addr,
                        message_parser_ptr &parser,
                        bool is_client);
    DSN_API virtual ~rpc_session();

    virtual void close_on_fault_injection() = 0;

    DSN_API bool has_pending_out_msgs();
    bool is_client() const { return _is_client; }
    ::dsn::rpc_address remote_address() const { return _remote_addr; }
    connection_oriented_network &net() const { return _net; }
    message_parser_ptr parser() const { return _parser; }
    DSN_API void send_message(message_ex *msg);
    DSN_API bool cancel(message_ex *request);
    void delay_recv(int delay_ms);
    DSN_API bool on_recv_message(message_ex *msg, int delay_ms);

    // for client session
public:
    // return true if the socket should be closed
    DSN_API bool on_disconnected(bool is_write);

    virtual void connect() = 0;

    // for server session
public:
    DSN_API void start_read_next(int read_next = 256);

    // should be called in do_read() before using _parser when it is nullptr.
    // returns:
    //   -1 : prepare failed, maybe because of invalid message header type
    //    0 : prepare succeed, _parser is not nullptr now.
    //   >0 : need read more data, returns read_next.
    DSN_API int prepare_parser();

    // shared
protected:
    //
    // sending messages are put in _sending_msgs
    // buffer is prepared well in _sending_buffers
    // always call on_send_completed later
    //
    virtual void send(uint64_t signature) = 0;
    virtual void do_read(int read_next) = 0;

protected:
    DSN_API bool try_connecting(); // return true when it is permitted
    DSN_API void set_connected();
    DSN_API bool set_disconnected(); // return true when it is permitted
    bool is_disconnected() const { return _connect_state == SS_DISCONNECTED; }
    bool is_connecting() const { return _connect_state == SS_CONNECTING; }
    bool is_connected() const { return _connect_state == SS_CONNECTED; }
    DSN_API void on_send_completed(uint64_t signature = 0); // default value for nothing is sent

private:
    // return whether there are messages for sending; should always be called in lock
    DSN_API bool unlink_message_for_send();
    DSN_API void clear_send_queue(bool resend_msgs);

protected:
    // constant info
    connection_oriented_network &_net;
    ::dsn::rpc_address _remote_addr;
    int _max_buffer_block_count_per_send;
    message_reader _reader;
    message_parser_ptr _parser;

    // messages are currently being sent
    // also locked by _lock later
    std::vector<message_parser::send_buf> _sending_buffers;
    std::vector<message_ex *> _sending_msgs;

private:
    const bool _is_client;
    rpc_client_matcher *_matcher;

    enum session_state
    {
        SS_CONNECTING,
        SS_CONNECTED,
        SS_DISCONNECTED
    };

    // TODO: expose the queue to be customizable
    ::dsn::utils::ex_lock_nr _lock; // [
    volatile bool _is_sending_next;
    int _message_count; // count of _messages
    dlink _messages;
    volatile session_state _connect_state;
    uint64_t _message_sent;
    // ]

    std::atomic_int _delay_server_receive_ms;
};

// --------- inline implementation --------------
inline void rpc_session::delay_recv(int delay_ms)
{
    int old_delay_ms = _delay_server_receive_ms.load();
    while (delay_ms > old_delay_ms &&
           !_delay_server_receive_ms.compare_exchange_weak(old_delay_ms, delay_ms)) {
    }
}

/*@}*/
}
