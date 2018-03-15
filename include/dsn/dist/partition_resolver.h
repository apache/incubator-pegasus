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
 *     resolve URI as rpc addresses (IP or group)
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), first draft
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/service_api_cpp.h>
#include <dsn/utility/autoref_ptr.h>

namespace dsn {
namespace dist {
void register_partition_resolver_providers();

class partition_resolver : public ref_counter
{
public:
    /*! app resolve result. */
    struct resolve_result
    {
        error_code err;      ///< ERR_OK
                             ///< ERR_SERVICE_NOT_FOUND if resolver or app is missing
                             ///< ERR_IO_PENDING if resolve in is progress, callers
                             ///< should call resolve_async in this case
        rpc_address address; ///< IPv4 of the target to send request to
        dsn::gpid pid;       ///< global partition indentity
    };

public:
    template <typename T>
    static partition_resolver *create(rpc_address &meta_server, const char *app_path)
    {
        return new T(meta_server, app_path);
    }

    typedef partition_resolver *(*factory)(rpc_address &, const char *);

public:
    partition_resolver(rpc_address meta_server, const char *app_path)
        : _app_path(app_path), _meta_server(meta_server)
    {
    }

    virtual ~partition_resolver() {}

    /**
    * resolve partition_hash into IP or group addresses to know what to connect next
    *
    * \param partition_hash the partition hash
    * \param callback       callback invoked on completion or timeout
    * \param timeout_ms     timeout to execute the callback
    *
    * \return see \ref resolve_result for details
    */
    virtual void
    resolve(uint64_t partition_hash,
            std::function<void(dist::partition_resolver::resolve_result &&)> &&callback,
            int timeout_ms) = 0;

    /*!
     failure handler when access failed for certain partition

     \param partition_index zero-based index of the partition.
     \param err             error code

     this is usually to trigger new round of address resolve
     */
    virtual void on_access_failure(int partition_index, error_code err) = 0;

    /**
     * get zero-based partition index
     *
     * \param partition_count number of partitions.
     * \param partition_hash  the partition hash.
     *
     * \return zero-based partition index.
     */

    virtual int get_partition_index(int partition_count, uint64_t partition_hash) = 0;

    std::string get_app_path() const { return _app_path; }

    ::dsn::rpc_address get_meta_server() const { return _meta_server; }

protected:
    std::string _app_path;
    rpc_address _meta_server;
};

typedef ref_ptr<partition_resolver> partition_resolver_ptr;
}
}
