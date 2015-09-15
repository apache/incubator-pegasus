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

# include <dsn/internal/task.h>
# include <dsn/internal/synchronize.h>
# include <dsn/internal/message_parser.h>
# include <dsn/cpp/address.h>
# include <dsn/internal/priority_queue.h>

namespace dsn {

    class rpc_engine;
    class service_node;
    class task_worker_pool;
    class task_queue;

    //
    // network bound to a specific rpc_channel and port (see start)
    // !!! all threads must be started with task::set_tls_dsn_context(null, provider->node());
    //
    class network
    {
    public:
        //
        // network factory prototype
        //
        template <typename T> static network* create(rpc_engine* srv, network* inner_provider)
        {
            return new T(srv, inner_provider);
        }

    public:
        //
        // srv - the rpc engine, could contain many networks there
        // inner_provider - when not null, this network is simply a wrapper for tooling purpose (e.g., tracing)
        //                  all downcalls should be redirected to the inner provider in the end
        //
        network(rpc_engine* srv, network* inner_provider); 
        virtual ~network() {}

        //
        // when client_only is true, port is faked (equal to app id for tracing purpose)
        //
        virtual error_code start(rpc_channel channel, int port, bool client_only, io_modifer& ctx) = 0;

        //
        // the named address (when client_only is true)
        //
        virtual const ::dsn::rpc_address& address() = 0;

        //
        // this is where the upper rpc engine calls down for a RPC call
        //   request - the message to be sent, all meta info (e.g., timeout, server address are
        //             prepared ready in its header; use message_parser to extract
        //             blobs from message for sending
        //   call - the RPC response callback task to be invoked either timeout or response is received
        //          null when this is a one-way RPC call.
        //
        virtual void call(message_ex* request, rpc_response_task* call) = 0;
                
        //
        // utilities
        //
        service_node* node() const;

        //
        // called when network received a complete message
        //
        void on_recv_request(message_ex* msg, int delay_ms);
        
        //
        // create a client matcher for matching RPC request and RPC response,
        // see rpc_client_matcher for details
        //
        rpc_client_matcher_ptr new_client_matcher();

        //
        // create a message parser for
        //  (1) extracing blob from a RPC request message for low layer'
        //  (2) parsing a incoming blob message to get the rpc_message
        //
        std::shared_ptr<message_parser> new_message_parser();


        int max_buffer_block_count_per_send() const { return _max_buffer_block_count_per_send; }

    protected:
        rpc_engine                    *_engine;
        network_header_format         _parser_type;
        int                           _message_buffer_block_size;
        int                           _max_buffer_block_count_per_send;

    private:
        friend class rpc_engine;
        void reset_parser(network_header_format name, int message_buffer_block_size);
    };

    //
    // client matcher for matching RPC request and RPC response, and handling timeout
    // (1) the whole network may share a single client matcher,
    // (2) or we usually prefere each <src, dst> pair use a client matcher to have better inquery performance
    // (3) or we have certain cases we want RPC responses from node which is not the initial target node
    //     the RPC request message is sent to. In this case, a shared rpc_engine level matcher is used.
    //
    #define MATCHER_BUCKET_NR 13
    class rpc_client_matcher : public ref_counter
    {
    public:
        ~rpc_client_matcher();

        //
        // when a two-way RPC call is made, register the requst id and the callback
        // which also registers a timer for timeout tracking
        //
        void on_call(message_ex* request, rpc_response_task* call);

        //
        // when a RPC response is received, call this function to trigger calback
        //  key - message.header.id
        //  reply - rpc response message
        //  delay_ms - sometimes we want to delay the delivery of the message for certain purposes
        //
        bool on_recv_reply(uint64_t key, message_ex* reply, int delay_ms);

    private:
        friend class rpc_timeout_task;
        void on_rpc_timeout(uint64_t key);

    private:
        struct match_entry
        {
            rpc_response_task*    resp_task;
            task*                 timeout_task;
        };
        typedef std::unordered_map<uint64_t, match_entry> rpc_requests;
        rpc_requests                  _requests[MATCHER_BUCKET_NR];
        ::dsn::utils::ex_lock_nr_spin _requests_lock[MATCHER_BUCKET_NR];
    };
    
    //
    // an incomplete network implementation for connection oriented network, e.g., TCP
    //
    class connection_oriented_network : public network
    {
    public:
        connection_oriented_network(rpc_engine* srv, network* inner_provider);
        virtual ~connection_oriented_network() {}

        // server session management
        rpc_session_ptr get_server_session(const ::dsn::rpc_address& ep);
        void on_server_session_accepted(rpc_session_ptr& s);
        void on_server_session_disconnected(rpc_session_ptr& s);

        // client session management
        rpc_session_ptr get_client_session(const ::dsn::rpc_address& ep);
        void on_client_session_disconnected(rpc_session_ptr& s);

        // called upon RPC call, rpc client session is created on demand
        virtual void call(message_ex* request, rpc_response_task* call);

        // to be defined
        virtual rpc_session_ptr create_client_session(const ::dsn::rpc_address& server_addr) = 0;

    protected:
        typedef std::unordered_map<::dsn::rpc_address, rpc_session_ptr> client_sessions;
        client_sessions               _clients;
        utils::rw_lock_nr             _clients_lock;

        typedef std::unordered_map<::dsn::rpc_address, rpc_session_ptr> server_sessions;
        server_sessions               _servers;
        utils::rw_lock_nr             _servers_lock;
    };

    //
    // session managements (both client and server types)
    //
    class rpc_session : public ref_counter
    {
    public:
        virtual ~rpc_session();
                
        bool has_pending_out_msgs();
        bool is_client() const { return _matcher.get() != nullptr; }
        const ::dsn::rpc_address& remote_address() const { return _remote_addr; }
        connection_oriented_network& net() const { return _net; }
        void send_message(message_ex* msg);

    // for client session
    public:        
        rpc_session(
            connection_oriented_network& net, 
            const ::dsn::rpc_address& remote_addr, 
            rpc_client_matcher_ptr& matcher,
            std::shared_ptr<message_parser>& parser
            );
        bool on_recv_reply(uint64_t key, message_ex* reply, int delay_ms);
        bool on_disconnected();
        void call(message_ex* request, rpc_response_task* call);
        
        virtual void connect() = 0;
        
    // for server session
    public:        
        rpc_session(
            connection_oriented_network& net, 
            const ::dsn::rpc_address& remote_addr,
            std::shared_ptr<message_parser>& parser
            );
        void on_recv_request(message_ex* msg, int delay_ms);

    // shared
    protected:        
        // send msgs (double-linked list)
        // always call on_send_completed later
        virtual void send(message_ex* msgs) = 0;

    protected:
        bool try_connecting(); // return true when it is permitted
        void set_connected();
        void set_disconnected();
        bool is_disconnected() const { return _connect_state == SS_DISCONNECTED; }
        bool is_connecting() const { return _connect_state == SS_CONNECTING; }
        bool is_connected() const { return _connect_state == SS_CONNECTED; }        
        void on_send_completed(message_ex* msg);

    private:
        message_ex* unlink_message_for_send();

    protected:
        connection_oriented_network        &_net;
        ::dsn::rpc_address                 _remote_addr;
        std::atomic<int>                   _reconnect_count_after_last_success;
        rpc_client_matcher_ptr             _matcher; // client used only
        int                                _max_buffer_block_count_per_send;        
        std::shared_ptr<dsn::message_parser> _parser;

        std::vector<message_parser::send_buf> _sending_buffers;
        message_ex                         *_sending_msgs;

    private:
        enum session_state
        {
            SS_CONNECTING,
            SS_CONNECTED,
            SS_DISCONNECTED
        };

        // TODO: expose the queue to be customizable
        ::dsn::utils::ex_lock_nr           _lock;
        bool                               _is_sending_next;
        dlink                              _messages;        
        session_state                      _connect_state;
        uint64_t                           _message_sent;        
    };
}
