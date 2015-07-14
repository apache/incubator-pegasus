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
# include <dsn/internal/network.h>
# include <dsn/internal/factory_store.h>
# include "rpc_engine.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "rpc_session"

namespace dsn {

    rpc_client_session::rpc_client_session(connection_oriented_network& net, const end_point& remote_addr, rpc_client_matcher_ptr& matcher)
        : _net(net), _remote_addr(remote_addr), _matcher(matcher)
    {
        _disconnected = false;
    }

    void rpc_client_session::call(message_ptr& request, rpc_response_task_ptr& call)
    {
        if (call != nullptr)
        {
            _matcher->on_call(request, call);
        }

        send(request);
    }

    void rpc_client_session::on_disconnected()
    {
        _disconnected = true;

        rpc_client_session_ptr sp = this;
        _net.on_client_session_disconnected(sp);
    }

    bool rpc_client_session::on_recv_reply(uint64_t key, message_ptr& reply, int delay_ms)
    {
        if (reply != nullptr)
        {
            reply->header().from_address = remote_address();
            reply->header().to_address = _net.address();
        }

        return _matcher->on_recv_reply(key, reply, delay_ms);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    rpc_server_session::rpc_server_session(connection_oriented_network& net, const end_point& remote_addr)
        : _remote_addr(remote_addr), _net(net)
    {
    }

    void rpc_server_session::on_recv_request(message_ptr& msg, int delay_ms)
    {
        msg->header().from_address = remote_address();
        msg->header().from_address.port = msg->header().client.port;
        msg->header().to_address = _net.address();

        msg->server_session().reset(this);
        return _net.on_recv_request(msg, delay_ms);
    }

    void rpc_server_session::on_disconnected()
    {
        rpc_server_session_ptr sp = this;
        return _net.on_server_session_disconnected(sp);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    network::network(rpc_engine* srv, network* inner_provider)
        : _engine(srv), _parser_type(NET_HDR_DSN)
    {   
        _message_buffer_block_size = 1024 * 64;
    }

    void network::reset_parser(network_header_format name, int message_buffer_block_size)
    {
        _message_buffer_block_size = message_buffer_block_size;
        _parser_type = name;
    }

    service_node* network::node() const
    {
        return _engine->node();
    }

    void network::on_recv_request(message_ptr& msg, int delay_ms)
    {
        return _engine->on_recv_request(msg, delay_ms);
    }
    
    rpc_client_matcher_ptr network::new_client_matcher()
    {
        return rpc_client_matcher_ptr(new rpc_client_matcher());
    }

    std::shared_ptr<message_parser> network::new_message_parser()
    {
        message_parser * parser = utils::factory_store<message_parser>::create(_parser_type.to_string(), PROVIDER_TYPE_MAIN, _message_buffer_block_size);
        dassert(parser, "message parser '%s' not registerd or invalid!", _parser_type.to_string());
        return std::shared_ptr<message_parser>(parser);
    }

    connection_oriented_network::connection_oriented_network(rpc_engine* srv, network* inner_provider)
        : network(srv, inner_provider)
    {
    }

    void connection_oriented_network::call(message_ptr& request, rpc_response_task_ptr& call)
    {
        rpc_client_session_ptr client = nullptr;
        end_point& to = request->header().to_address;
        bool new_client = false;

        // TODO: thread-local client ptr cache
        {
            utils::auto_read_lock l(_clients_lock);
            auto it = _clients.find(to);
            if (it != _clients.end())
            {
                client = it->second;
            }
        }

        if (nullptr == client.get())
        {
            utils::auto_write_lock l(_clients_lock);
            auto it = _clients.find(to);
            if (it != _clients.end())
            {
                client = it->second;
            }
            else
            {
                client = create_client_session(to);
                _clients.insert(client_sessions::value_type(to, client));
                new_client = true;
            }
        }

        // init connection if necessary
        if (new_client) 
            client->connect();

        // rpc call
        client->call(request, call);
    }

    rpc_server_session_ptr connection_oriented_network::get_server_session(const end_point& ep)
    {
        utils::auto_read_lock l(_servers_lock);
        auto it = _servers.find(ep);
        return it != _servers.end() ? it->second : nullptr;
    }

    void connection_oriented_network::on_server_session_accepted(rpc_server_session_ptr& s)
    {
        dinfo("server session %s:%d accepted", s->remote_address().name.c_str(), static_cast<int>(s->remote_address().port));

        utils::auto_write_lock l(_servers_lock);
        _servers.insert(server_sessions::value_type(s->remote_address(), s));

    }

    void connection_oriented_network::on_server_session_disconnected(rpc_server_session_ptr& s)
    {
        bool r = false;
        {
            utils::auto_write_lock l(_servers_lock);
            auto it = _servers.find(s->remote_address());
            if (it != _servers.end() && it->second.get() == s.get())
            {
                _servers.erase(it);
                r = true;
            }                
        }

        if (r)
        {
            dinfo("server session %s:%d disconnected", 
                s->remote_address().name.c_str(),
                static_cast<int>(s->remote_address().port));
        }
    }

    rpc_client_session_ptr connection_oriented_network::get_client_session(const end_point& ep)
    {
        utils::auto_read_lock l(_clients_lock);
        auto it = _clients.find(ep);
        return it != _clients.end() ? it->second : nullptr;
    }

    void connection_oriented_network::on_client_session_disconnected(rpc_client_session_ptr& s)
    {
        bool r = false;
        {
            utils::auto_write_lock l(_clients_lock);
            auto it = _clients.find(s->remote_address());
            if (it != _clients.end() && it->second.get() == s.get())
            {
                _clients.erase(it);
                r = true;
            }
        }

        if (r)
        {
            dinfo("client session %s:%d disconnected", s->remote_address().name.c_str(), 
                static_cast<int>(s->remote_address().port));
        }
    }
}
