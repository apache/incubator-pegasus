# include <rdsn/internal/network.h>
# include "rpc_engine.h"

# define __TITLE__ "rpc_session"

namespace rdsn {

    rpc_client_session::rpc_client_session(network& net, const end_point& remote_addr, std::shared_ptr<rpc_client_matcher>& matcher)
        : _net(net), _remote_addr(remote_addr), _matcher(matcher)
    {
    }

    void rpc_client_session::call(message_ptr& request, rpc_response_task_ptr& call)
    {
        if (call != nullptr)
        {
            auto sthis = shared_from_this();
            _matcher->on_call(request, call, sthis);
        }

        send(request);
    }

    void rpc_client_session::on_disconnected()
    {
        auto s = shared_from_this();
        _net.on_client_session_disconnected(s);
    }

    bool rpc_client_session::on_recv_reply(uint64_t key, message_ptr& reply, int delay_handling_milliseconds)
    {
        if (reply != nullptr)
        {
            reply->header().from_address = remote_address();
            reply->header().to_address = _net.address();
        }

        return _matcher->on_recv_reply(key, reply, delay_handling_milliseconds);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    rpc_server_session::rpc_server_session(network& net, const end_point& remote_addr)
        : _remote_addr(remote_addr), _net(net)
    {
    }

    void rpc_server_session::on_recv_request(message_ptr& msg, int delay_handling_milliseconds)
    {
        msg->header().from_address = remote_address();
		msg->header().from_address.port = msg->header().client_port;
        msg->header().to_address = _net.address();

        msg->server_session() = shared_from_this();
        return _net.engine()->on_recv_request(msg, delay_handling_milliseconds);
    }

    void rpc_server_session::on_disconnected()
    {
        auto s = shared_from_this();
        return _net.on_server_session_disconnected(s);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

	int network::max_faked_port_for_client_only_node = 0;

    network::network(rpc_engine* srv, network* inner_provider)
        : _engine(srv)
    {
    }
    
    std::shared_ptr<rpc_client_matcher> network::new_client_matcher()
    {
        return std::shared_ptr<rpc_client_matcher>(new rpc_client_matcher());
    }

    void network::call(message_ptr& request, rpc_response_task_ptr& call)
    {
        std::shared_ptr<rpc_client_session> client = nullptr;
        end_point& to = request->header().to_address;

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

                // init connection
                client->connect();
            }
        }

        client->call(request, call);
        // TODO: periodical check to remove idle clients
    }

    std::shared_ptr<rpc_server_session> network::get_server_session(const end_point& ep)
    {
        utils::auto_read_lock l(_servers_lock);
        auto it = _servers.find(ep);
        return it != _servers.end() ? it->second : nullptr;
    }

    void network::on_server_session_accepted(std::shared_ptr<rpc_server_session>& s)
    {
        utils::auto_write_lock l(_servers_lock);
        _servers.insert(server_sessions::value_type(s->remote_address(), s));
    }

    void network::on_server_session_disconnected(std::shared_ptr<rpc_server_session>& s)
    {
        utils::auto_write_lock l(_servers_lock);
        auto it = _servers.find(s->remote_address());
        if (it != _servers.end() && it->second.get() == s.get())
            _servers.erase(it);
    }

    std::shared_ptr<rpc_client_session> network::get_client_session(const end_point& ep)
    {
        utils::auto_read_lock l(_clients_lock);
        auto it = _clients.find(ep);
        return it != _clients.end() ? it->second : nullptr;
    }

    void network::on_client_session_disconnected(std::shared_ptr<rpc_client_session>& s)
    {
        utils::auto_write_lock l(_clients_lock);
        auto it = _clients.find(s->remote_address());
        if (it != _clients.end() && it->second.get() == s.get())
            _clients.erase(it);
    }
}
