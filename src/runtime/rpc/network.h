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

#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "perf_counter/perf_counter_wrapper.h"
#include "rpc_address.h"
#include "runtime/rpc/message_parser.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fmt_utils.h"
#include "utils/join_point.h"
#include "utils/link.h"
#include "utils/synchronize.h"

namespace dsn {

class rpc_engine;
class service_node;

/*!
@addtogroup tool-api-providers
@{
*/

/*!
  network bound to a specific rpc_channel and port (see start)
 !!! all threads must be started with task::set_tls_dsn_context(provider->node(), null);
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
    network(rpc_engine *srv, network *inner_provider);
    virtual ~network() {}

    //
    // when client_only is true, port is faked (equal to app id for tracing purpose)
    //
    virtual error_code start(rpc_channel channel, int port, bool client_only) = 0;

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
    service_node *node() const;

    //
    // called when network received a complete request message
    //
    void on_recv_request(message_ex *msg, int delay_ms);

    //
    // called when network received a complete reply message or network failed,
    // if network failed, the 'msg' will be nullptr
    //
    void on_recv_reply(uint64_t id, message_ex *msg, int delay_ms);

    //
    // create a message parser for
    //  (1) extracing blob from a RPC request message for low layer'
    //  (2) parsing a incoming blob message to get the rpc_message
    //
    message_parser *new_message_parser(network_header_format hdr_format);

    rpc_engine *engine() const { return _engine; }
    int max_buffer_block_count_per_send() const { return _max_buffer_block_count_per_send; }
    network_header_format client_hdr_format() const { return _client_hdr_format; }
    network_header_format unknown_msg_hdr_format() const { return _unknown_msg_header_format; }
    int message_buffer_block_size() const { return _message_buffer_block_size; }

    static uint32_t get_local_ipv4();

protected:
    rpc_engine *_engine;
    network_header_format _client_hdr_format;
    network_header_format _unknown_msg_header_format; // default is NET_HDR_INVALID
    int _message_buffer_block_size;
    int _max_buffer_block_count_per_send;

private:
    friend class rpc_engine;
    void reset_parser_attr(network_header_format client_hdr_format, int message_buffer_block_size);
};

/*!
  an incomplete network implementation for connection oriented network, e.g., TCP
*/
class connection_oriented_network : public network
{
public:
    connection_oriented_network(rpc_engine *srv, network *inner_provider);
    virtual ~connection_oriented_network() {}

    // server session management
    rpc_session_ptr get_server_session(::dsn::rpc_address ep);
    void on_server_session_accepted(rpc_session_ptr &s);
    void on_server_session_disconnected(rpc_session_ptr &s);

    // Checks if IP of the incoming session has too much connections.
    // Related config: [network] conn_threshold_per_ip. No limit if the value is 0.
    bool check_if_conn_threshold_exceeded(::dsn::rpc_address ep);

    // client session management
    void on_client_session_connected(rpc_session_ptr &s);
    void on_client_session_disconnected(rpc_session_ptr &s);

    // called upon RPC call, rpc client session is created on demand
    virtual void send_message(message_ex *request) override;

    // called by rpc engine
    virtual void inject_drop_message(message_ex *msg, bool is_send) override;

    // to be defined
    virtual rpc_session_ptr create_client_session(::dsn::rpc_address server_addr) = 0;

protected:
    typedef std::unordered_map<::dsn::rpc_address, rpc_session_ptr> client_sessions;
    client_sessions _clients; // to_address => rpc_session
    utils::rw_lock_nr _clients_lock;

    typedef std::unordered_map<::dsn::rpc_address, rpc_session_ptr> server_sessions;
    server_sessions _servers; // from_address => rpc_session
    typedef std::unordered_map<uint32_t, uint32_t> ip_connection_count;
    ip_connection_count _ip_conn_count; // from_ip => connection count
    utils::rw_lock_nr _servers_lock;

    perf_counter_wrapper _client_session_count;
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
    static join_point<void, rpc_session *> on_rpc_session_connected;
    static join_point<void, rpc_session *> on_rpc_session_disconnected;
    static join_point<bool, message_ex *> on_rpc_recv_message;
    static join_point<bool, message_ex *> on_rpc_send_message;
    /*@}*/
public:
    rpc_session(connection_oriented_network &net,
                ::dsn::rpc_address remote_addr,
                message_parser_ptr &parser,
                bool is_client);
    virtual ~rpc_session();

    virtual void connect() = 0;
    virtual void close() = 0;

    // Whether this session is launched on client side.
    bool is_client() const { return _is_client; }

    dsn::rpc_address remote_address() const { return _remote_addr; }
    connection_oriented_network &net() const { return _net; }
    message_parser_ptr parser() const { return _parser; }

    ///
    /// rpc_session's interface for sending and receiving
    ///
    void send_message(message_ex *msg);
    bool cancel(message_ex *request);
    bool delay_recv(int delay_ms);
    bool on_recv_message(message_ex *msg, int delay_ms);
    /// ret value:
    ///    true  - pend succeed
    ///    false - pend failed
    bool try_pend_message(message_ex *msg);
    void clear_pending_messages();

    /// interfaces for security authentication,
    /// you can ignore them if you don't enable auth
    void set_negotiation_succeed();
    bool is_negotiation_succeed() const;

    void set_client_username(const std::string &user_name);
    const std::string &get_client_username() const;

public:
    ///
    /// for subclass to implement receiving message
    ///
    void start_read_next(int read_next = 256);
    // should be called in do_read() before using _parser when it is nullptr.
    // returns:
    //   -1 : prepare failed, maybe because of invalid message header type
    //    0 : prepare succeed, _parser is not nullptr now.
    //   >0 : need read more data, returns read_next.
    int prepare_parser();
    virtual void do_read(int read_next) = 0;

    ///
    /// for subclass to implement sending message
    ///
    // return whether there are messages for sending;
    // should always be called in lock
    bool unlink_message_for_send();
    virtual void send(uint64_t signature) = 0;
    void on_send_completed(uint64_t signature = 0);
    virtual void on_failure(bool is_write = false);

protected:
    ///
    /// fields related to sending messages
    ///
    enum session_state
    {
        SS_CONNECTING,
        SS_CONNECTED,
        SS_DISCONNECTED
    };
    friend USER_DEFINED_ENUM_FORMATTER(rpc_session::session_state);

    mutable utils::ex_lock_nr _lock; // [
    volatile session_state _connect_state;

    bool negotiation_succeed = false;
    // when the negotiation of a session isn't succeed,
    // all messages are queued in _pending_messages.
    // after connected, all of them are moved to "_messages"
    std::vector<message_ex *> _pending_messages;

    // messages are sent in batch, firstly all messages are linked together
    // in a doubly-linked list "_messages".
    // if no messages are on-the-flying, a batch of messages are fetch from the "_messages"
    // and put them to _sending_msgs; meanwhile, buffers of these messages are put
    // in _sending_buffers
    dlink _messages;
    int _message_count; // count of _messages

    bool _is_sending_next;

    std::vector<message_ex *> _sending_msgs;
    std::vector<message_parser::send_buf> _sending_buffers;

    uint64_t _message_sent;
    // ]

protected:
    ///
    /// change status and check status
    ///
    // return true when it is permitted
    bool set_connecting();
    // return true when it is permitted
    bool set_disconnected();
    void set_connected();

    void clear_send_queue(bool resend_msgs);
    bool on_disconnected(bool is_write);

protected:
    // constant info
    connection_oriented_network &_net;
    dsn::rpc_address _remote_addr;
    int _max_buffer_block_count_per_send;
    message_reader _reader;
    message_parser_ptr _parser;

private:
    const bool _is_client;
    rpc_client_matcher *_matcher;

    std::atomic_int _delay_server_receive_ms;

    // _client_username is only valid if it is a server rpc_session.
    // it represents the name of the corresponding client
    std::string _client_username;
};

// --------- inline implementation --------------
// return true if delay applied.
inline bool rpc_session::delay_recv(int delay_ms)
{
    bool exchanged = false;
    int old_delay_ms = _delay_server_receive_ms.load();
    while (!exchanged && delay_ms > old_delay_ms) {
        exchanged = _delay_server_receive_ms.compare_exchange_weak(old_delay_ms, delay_ms);
    }
    return exchanged;
}

/*@}*/
} // namespace dsn
