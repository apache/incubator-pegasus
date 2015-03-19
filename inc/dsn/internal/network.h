/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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
    
    class network
    {
    public:
        template <typename T> static network* create(rpc_engine* srv, network* inner_provider)
        {
            return new T(srv, inner_provider);
        }

    public:
        network(rpc_engine* srv, network* inner_provider); 
        virtual ~network() {}

        service_node* node() const;
        rpc_engine* engine() const { return _engine;  }

        std::shared_ptr<rpc_client_matcher> new_client_matcher();
        std::shared_ptr<message_parser> new_message_parser();

        rpc_server_session_ptr get_server_session(const end_point& ep);
        void on_server_session_accepted(rpc_server_session_ptr& s);
        void on_server_session_disconnected(rpc_server_session_ptr& s);

        rpc_client_session_ptr get_client_session(const end_point& ep);
        void on_client_session_disconnected(rpc_client_session_ptr& s);

        virtual error_code start(int port, bool client_only) = 0;
        virtual const end_point& address() = 0;
        virtual rpc_client_session_ptr create_client_session(const end_point& server_addr) = 0;

        // used by rpc_engine only
        void reset_parser(const std::string& name, int message_buffer_block_size);
        void call(message_ptr& request, rpc_response_task_ptr& call);

    protected:
        rpc_engine                    *_engine;
        std::string                   _parser_type;
        int                           _message_buffer_block_size;
        
        typedef std::map<end_point, rpc_client_session_ptr> client_sessions;
        client_sessions               _clients;
        utils::rw_lock                _clients_lock;

        typedef std::map<end_point, rpc_server_session_ptr> server_sessions;
        server_sessions               _servers;
        utils::rw_lock                _servers_lock;

    public:
        static int max_faked_port_for_client_only_node;
    };


    class rpc_client_session : public ref_object
    {
    public:
        rpc_client_session(network& net, const end_point& remote_addr, std::shared_ptr<rpc_client_matcher>& matcher);
        bool on_recv_reply(uint64_t key, message_ptr& reply, int delay_handling_milliseconds = 0);
        void on_disconnected();
        void call(message_ptr& request, rpc_response_task_ptr& call);
        const end_point& remote_address() const { return _remote_addr; }

        virtual void connect() = 0;
        virtual void send(message_ptr& msg) = 0;

    protected:
        network                  &_net;
        end_point                 _remote_addr;
        std::shared_ptr<rpc_client_matcher> _matcher;
    };

    DEFINE_REF_OBJECT(rpc_client_session)

    class rpc_server_session : public ref_object
    {
    public:
        rpc_server_session(network& net, const end_point& remote_addr);
        void on_recv_request(message_ptr& msg, int delay_handling_milliseconds = 0);
        void on_disconnected();
        const end_point& remote_address() const { return _remote_addr; }

        virtual void send(message_ptr& reply_msg) = 0;

    protected:
        network&   _net;
        end_point _remote_addr;
    };

    DEFINE_REF_OBJECT(rpc_server_session)
}
