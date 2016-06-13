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
 *     rpc-address that wrap and solve URI
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/cpp/address.h>
# include <dsn/dist/partition_resolver.h>
# include <algorithm> // for std::find()
# include <dsn/internal/configuration.h>

namespace dsn
{
    /** A RPC URI address. */
    class rpc_uri_address
    {
    public:

        /**
         * Constructor.
         *
         * \param uri URI of the document, it is composed with three parts:
         *            dsn://meta-server:23356/app1
         *          protocol : // resolver-address / app-path
         */
        rpc_uri_address(const char* uri);

        ~rpc_uri_address();
        
        /**
        * Gets URI address components <resolver-address, app-path>, e.g.,
        * given dsn://meta-address:8080/app-path
        * return <dsn://meta-address:8080, app-path>
        *
        * \return The resolver address
        */
        std::pair<std::string, std::string> get_uri_components();

        const char* uri() const { return _uri.c_str(); }

        dist::partition_resolver_ptr get_resolver() { return _resolver; }

    private:
        ::dsn::dist::partition_resolver_ptr _resolver;
        std::string _uri;
        rpc_address _uri_address;
    };


    class uri_resolver
    {
    public:
        /**
        * Constructor.
        *
        * \param name      resolver name, e.g., dsn://meta-server:port
        * \param factory   factory for creating partition_resolver
        * \param arguments end-point list which composes the meta-server group,
        *                  e.g., host1:port1,host2:port2,host3:port3
        */
        uri_resolver(const char* name, const char* factory, const char* arguments);

        ~uri_resolver();
        
        dist::partition_resolver_ptr get_app_resolver(const char* app);

    private:
        std::unordered_map<std::string, dist::partition_resolver_ptr > _apps; ///< app-path to app-resolver map
        service::zrwlock_nr _apps_lock;

        rpc_address _meta_server;
        std::string _name;
        std::string _factory;
    };

    class uri_resolver_manager
    {
    public:
        uri_resolver_manager(configuration* config);        

        std::shared_ptr<uri_resolver> get(rpc_uri_address* uri) const;

    private:
        void setup_resolvers(configuration* config);

        typedef std::unordered_map<std::string, std::shared_ptr<uri_resolver> > resolvers;
        resolvers _resolvers;
        mutable utils::rw_lock_nr _lock;
    };


    // ------------------ inline implementation --------------------
    inline uri_resolver_manager::uri_resolver_manager(configuration* config)
    {
        setup_resolvers(config);
    }
}

