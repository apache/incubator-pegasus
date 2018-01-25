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
 *     network file system component base interface
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/service_api_c.h>
#include <string>
#include <dsn/utility/utils.h>
#include <dsn/utility/binary_reader.h>
#include <dsn/utility/binary_writer.h>
#include <dsn/tool-api/task.h>

namespace dsn {

/*!
@addtogroup tool-api-providers
@{
*/

struct remote_copy_request
{
    ::dsn::rpc_address source;
    std::string source_dir;
    std::vector<std::string> files;
    std::string dest_dir;
    bool overwrite;
    bool high_priority;
};

struct remote_copy_response
{
};

DSN_API extern void marshall(::dsn::binary_writer &writer, const remote_copy_request &val);

DSN_API extern void unmarshall(::dsn::binary_reader &reader, /*out*/ remote_copy_request &val);

class service_node;
class task_worker_pool;
class task_queue;

class nfs_node
{
public:
    template <typename T>
    static nfs_node *create(service_node *node)
    {
        return new T(node);
    }

    typedef nfs_node *(*factory)(service_node *);

public:
    nfs_node(service_node *node) : _node(node) {}

    virtual ~nfs_node() {}

    virtual ::dsn::error_code start(io_modifer &ctx) = 0;

    virtual error_code stop() = 0;

    virtual void call(std::shared_ptr<remote_copy_request> rci, aio_task *callback) = 0;

    service_node *node() { return _node; }

protected:
    service_node *_node;
};

/*@}*/
}
