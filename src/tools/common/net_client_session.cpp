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
# include "net_client_session.h"
# include <dsn/internal/logging.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "net.session"

namespace dsn {
    namespace tools {
        net_client_session::net_client_session(
            asio_network_provider& net, 
            boost::asio::ip::tcp::socket& socket,
            const end_point& remote_addr, 
            rpc_client_matcher_ptr& matcher,
            std::shared_ptr<message_parser>& parser)
            : 
            _net(net),
            rpc_client_session(net, remote_addr, matcher),
            client_net_io(remote_addr, socket, parser)
        {   
        }
        
        net_client_session::~net_client_session()
        {
        }
    }
}


