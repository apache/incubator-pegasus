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

namespace dsn {

    class rpc_engine;
    class rpc_client_matcher;
    class service_node;
    
    //
    // network bound to a specific rpc_channel and port (see start)
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
        virtual error_code start(rpc_channel channel, int port, bool client_only) = 0;

        //
        // the named address (when client_only is true)
        //
        virtual const end_point& address() = 0;

        //
        // this is where the upper rpc engine calls down for a RPC call
        //   request - the message to be sent, all meta info (e.g., timeout, server address are
        //             prepared ready in its header; use message_parser to extract
        //             blobs from message for sending
        //   call - the RPC response callback task to be invoked either timeout or response is received
        //          null when this is a one-way RPC call.
        //
        virtual void call(message_ptr& request, rpc_response_task_ptr& call) = 0;

        //
        // utilities
        //
        service_node* node() const;

        //
        // called when network received a complete message
        //
        void on_recv_request(message_ptr& msg, int delay_ms);
        
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

    protected:
        rpc_engine                    *_engine;
        network_header_format         _parser_type;
        int                           _message_buffer_block_size;

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
    class rpc_client_matcher : public ref_object
    {
    public:
        ~rpc_client_matcher();

        //
        // when a two-way RPC call is made, register the requst id and the callback
        // which also registers a timer for timeout tracking
        //
        void on_call(message_ptr& request, rpc_response_task_ptr& call);

        //
        // when a RPC response is received, call this function to trigger calback
        //  key - message.header.id
        //  reply - rpc response message
        //  delay_ms - sometimes we want to delay the delivery of the message for certain purposes
        //
        bool on_recv_reply(uint64_t key, message_ptr& reply, int delay_ms);

    private:
        friend class rpc_timeout_task;
        void on_rpc_timeout(uint64_t key);

    private:
        struct match_entry
        {
            rpc_response_task_ptr resp_task;
            task_ptr              timeout_task;
        };
        typedef std::unordered_map<uint64_t, match_entry> rpc_requests;
        rpc_requests                  _requests;
        ::dsn::utils::ex_lock_nr_spin _requests_lock;
    };

    DEFINE_REF_OBJECT(rpc_client_matcher)

    //
    // an incomplete network implementation for connection oriented network, e.g., TCP
    //
    class connection_oriented_network : public network
    {
    public:
        connection_oriented_network(rpc_engine* srv, network* inner_provider);
        virtual ~connection_oriented_network() {}

        // server session management
        rpc_server_session_ptr get_server_session(const end_point& ep);
        void on_server_session_accepted(rpc_server_session_ptr& s);
        void on_server_session_disconnected(rpc_server_session_ptr& s);

        // client session management
        rpc_client_session_ptr get_client_session(const end_point& ep);
        void on_client_session_disconnected(rpc_client_session_ptr& s);

        // called upon RPC call, rpc client session is created on demand
        virtual void call(message_ptr& request, rpc_response_task_ptr& call);

        // to be defined
        virtual rpc_client_session_ptr create_client_session(const end_point& server_addr) = 0;

    protected:
        typedef std::unordered_map<end_point, rpc_client_session_ptr> client_sessions;
        client_sessions               _clients;
        utils::rw_lock_nr             _clients_lock;

        typedef std::unordered_map<end_point, rpc_server_session_ptr> server_sessions;
        server_sessions               _servers;
        utils::rw_lock_nr             _servers_lock;
    };

    //
    // session management on the client side
    //
    class rpc_client_session : public ref_object
    {
    public:
        rpc_client_session(connection_oriented_network& net, const end_point& remote_addr, rpc_client_matcher_ptr& matcher);
        bool on_recv_reply(uint64_t key, message_ptr& reply, int delay_ms);
        void on_disconnected();
        void call(message_ptr& request, rpc_response_task_ptr& call);
        const end_point& remote_address() const { return _remote_addr; }
        connection_oriented_network& net() const { return _net; }
        bool is_disconnected() const { return _disconnected; }

        virtual void connect() = 0;
        virtual void send(message_ptr& msg) = 0;

    private:
        bool _disconnected;

    protected:
        connection_oriented_network         &_net;
        end_point                           _remote_addr;
        rpc_client_matcher_ptr _matcher;
    };

    DEFINE_REF_OBJECT(rpc_client_session)

    //
    // session management on the server side
    //
    class rpc_server_session : public ref_object
    {
    public:
        rpc_server_session(connection_oriented_network& net, const end_point& remote_addr);
        void on_recv_request(message_ptr& msg, int delay_ms);
        void on_disconnected();
        const end_point& remote_address() const { return _remote_addr; }

        virtual void send(message_ptr& reply_msg) = 0;

    protected:
        connection_oriented_network&   _net;
        end_point                      _remote_addr;
    };

    DEFINE_REF_OBJECT(rpc_server_session)
}
