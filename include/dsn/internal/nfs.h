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

# include <dsn/service_api.h>
# include <dsn/internal/nfs.client.impl.h>
# include <dsn/internal/nfs.server.impl.h>
# include <dsn/internal/nfs.node.impl.h>

namespace dsn {

	class nfs_node
	{
	public:
		template <typename T> static nfs_node* create(service_node* node)
		{
			return new T(node);
		}

	public:
		nfs_node(service_node* node) : _node(node) {}

		virtual void call(std::shared_ptr<remote_copy_request> rci, aio_task_ptr& callback)
		{
			_nfs_node_impl = new ::dsn::service::nfs_node_impl(rci->source, ::dsn::service::system::config());
			_nfs_node_impl->get_nfs_client_impl()->begin_remote_copy(rci, callback);
		}

		

	protected:
		service_node* _node;
		::dsn::service::nfs_node_impl* _nfs_node_impl;
	};
}
