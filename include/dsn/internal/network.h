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

# pragma once

# include <dsn/internal/task.h>
# include <dsn/internal/synchronize.h>
# include <dsn/internal/message_parser.h>
# include <dsn/cpp/address.h>
# include <dsn/internal/priority_queue.h>
# include <dsn/internal/exp_delay.h>

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
        
        typedef network* (*factory)(rpc_engine*, network*);

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
        // the named address
        //
        virtual ::dsn::rpc_address address() = 0;

        //
        // this is where the upper rpc engine calls down for a RPC call
        //   request - the message to be sent, all meta info (e.g., timeout, server address are
        //             prepared ready in its header; use message_parser to extract
        //             blobs from message for sending
        //
        virtual void send_message(message_ex* request) = 0;

        //
        // tools in rDSN may decide to drop this msg,
        // in this case, the network should implement the appropriate
        // failure model that makes this failure possible in reality
        //
        virtual void inject_drop_message(message_ex* msg, bool is_client, bool is_send) = 0;
                
        //
        // utilities
        //
        service_node* node() const;

        //
        // called when network received a complete message
        //
        void on_recv_request(message_ex* msg, int delay_ms);

        //
        //
        //
        void on_recv_reply(message_ex* msg, int delay_ms);
        
        //
        // create a message parser for
        //  (1) extracing blob from a RPC request message for low layer'
        //  (2) parsing a incoming blob message to get the rpc_message
        //
        std::unique_ptr<message_parser> new_message_parser();

        // for in-place new message parser
        std::pair<message_parser::factory2, size_t> get_message_parser_info();

        rpc_engine* engine() const { return _engine; }
        int max_buffer_block_count_per_send() const { return _max_buffer_block_count_per_send; }

    protected:
        static uint32_t get_local_ipv4();

    protected:
        rpc_engine                    *_engine;
        network_header_format         _parser_type;
        int                           _message_buffer_block_size;
        int                           _max_buffer_block_count_per_send;
        int                           _send_queue_threshold;

    private:
        friend class rpc_engine;
        void reset_parser(network_header_format name, int message_buffer_block_size);
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
        rpc_session_ptr get_server_session(::dsn::rpc_address ep);
        void on_server_session_accepted(rpc_session_ptr& s);
        void on_server_session_disconnected(rpc_session_ptr& s);

        // client session management
        rpc_session_ptr get_client_session(::dsn::rpc_address ep);
        void on_client_session_disconnected(rpc_session_ptr& s);

        // called upon RPC call, rpc client session is created on demand
        virtual void send_message(message_ex* request) override;

        // called by rpc engine
        virtual void inject_drop_message(message_ex* msg, bool is_client, bool is_send) override;

        // to be defined
        virtual rpc_session_ptr create_client_session(::dsn::rpc_address server_addr) = 0;
        
    protected:
        typedef std::unordered_map< ::dsn::rpc_address, rpc_session_ptr> client_sessions;
        client_sessions               _clients;
        utils::rw_lock_nr             _clients_lock;

        typedef std::unordered_map< ::dsn::rpc_address, rpc_session_ptr> server_sessions;
        server_sessions               _servers;
        utils::rw_lock_nr             _servers_lock;
    };

    //
    // session managements (both client and server types)
    //
    class rpc_client_matcher;
    class rpc_session : public ref_counter
    {
    public:
        rpc_session(
            connection_oriented_network& net,
            ::dsn::rpc_address remote_addr,
            std::unique_ptr<message_parser>&& parser,
            bool is_client
            );
        virtual ~rpc_session();

        virtual void close_on_fault_injection() = 0;
                
        bool has_pending_out_msgs();
        bool is_client() const { return _matcher != nullptr; }
        ::dsn::rpc_address remote_address() const { return _remote_addr; }
        connection_oriented_network& net() const { return _net; }
        void send_message(message_ex* msg);
        bool cancel(message_ex* request);
        bool pause_recv();
        void resume_recv();

    // for client session
    public:
        bool on_recv_reply(uint64_t key, message_ex* reply, int delay_ms);
        // return true if the socket should be closed
        bool on_disconnected();
               
        virtual void connect() = 0;
        
    // for server session
    public:
        void on_recv_request(message_ex* msg, int delay_ms);
        void start_read_next(int read_next = 256);

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
        bool try_connecting(); // return true when it is permitted
        void set_connected();
        void set_disconnected();
        bool is_disconnected() const { return _connect_state == SS_DISCONNECTED; }
        bool is_connecting() const { return _connect_state == SS_CONNECTING; }
        bool is_connected() const { return _connect_state == SS_CONNECTED; }        
        void on_send_completed(uint64_t signature = 0); // default value for nothing is sent

    private:
        // return whether there are messages for sending; should always be called in lock
        bool unlink_message_for_send();
        void clear(bool resend_msgs);

    protected:
        // constant info
        connection_oriented_network        &_net;
        ::dsn::rpc_address                 _remote_addr;
        int                                _max_buffer_block_count_per_send;        
        std::unique_ptr<dsn::message_parser> _parser;

        // messages are currently being sent
        // also locked by _lock later
        std::vector<message_parser::send_buf> _sending_buffers;
        std::vector<message_ex*>              _sending_msgs;

    private:
        std::atomic<int>                   _reconnect_count_after_last_success;
        rpc_client_matcher                 *_matcher; // client used only

        enum session_state
        {
            SS_CONNECTING,
            SS_CONNECTED,
            SS_DISCONNECTED
        };

        // TODO: expose the queue to be customizable
        std::atomic<int>                   _message_count;
        ::dsn::utils::ex_lock_nr           _lock; // [
        bool                               _is_sending_next;
        dlink                              _messages;        
        volatile session_state             _connect_state;
        uint64_t                           _message_sent;
        // ]

        // for throttling
        enum class recv_state
        {
            to_be_paused,
            paused,
            normal
        };
        std::atomic<recv_state>            _recv_state;
    };

    // --------- inline implementation ---------------
    inline bool rpc_session::pause_recv()
    {
        recv_state s = recv_state::normal;
        return _recv_state.compare_exchange_strong(s, recv_state::to_be_paused, std::memory_order_relaxed);
    }

    inline void rpc_session::resume_recv()
    {
        while (true)
        {
            recv_state s = recv_state::paused;
            if (_recv_state.compare_exchange_strong(s, recv_state::normal, std::memory_order_relaxed))
            {
                start_read_next();
                return;
            }

            // not paused yet
            else if (s == recv_state::to_be_paused)
            {
                // recover to normal, no real pause is done before
                if (_recv_state.compare_exchange_strong(s, recv_state::normal, std::memory_order_relaxed))
                {
                    return;
                }
                else
                {
                    // continue the next loop
                }
            }

            else
            {
                // s == recv_state::normal
                return;
            }
        }        
    }
}
