// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include <iostream>
#include "replica/replication_app_base.h"
#include "common/storage_serverlet.h"
#include <rrdb/rrdb.code.definition.h>

namespace pegasus {
namespace server {

typedef ::dsn::rpc_holder<::dsn::blob, ::dsn::apps::read_response> get_rpc;
typedef ::dsn::rpc_holder<dsn::apps::multi_get_request, dsn::apps::multi_get_response>
    multi_get_rpc;
typedef ::dsn::rpc_holder<::dsn::apps::batch_get_request, ::dsn::apps::batch_get_response>
    batch_get_rpc;
typedef ::dsn::rpc_holder<::dsn::blob, dsn::apps::count_response> sortkey_count_rpc;
typedef ::dsn::rpc_holder<::dsn::blob, dsn::apps::ttl_response> ttl_rpc;
typedef ::dsn::rpc_holder<::dsn::apps::get_scanner_request, dsn::apps::scan_response>
    get_scanner_rpc;
typedef ::dsn::rpc_holder<::dsn::apps::scan_request, dsn::apps::scan_response> scan_rpc;

class pegasus_read_service : public dsn::replication::replication_app_base,
                             public dsn::replication::storage_serverlet<pegasus_read_service>
{
public:
    pegasus_read_service(dsn::replication::replica *r) : dsn::replication::replication_app_base(r)
    {
    }
    virtual ~pegasus_read_service() {}
    virtual int on_request(dsn::message_ex *request) override
    {
        handle_request(request);
        return 0;
    }

protected:
    // all service handlers to be implemented further
    // RPC_RRDB_RRDB_GET
    virtual void on_get(get_rpc rpc) = 0;
    // RPC_RRDB_RRDB_MULTI_GET
    virtual void on_multi_get(multi_get_rpc rpc) = 0;
    // RPC_RRDB_RRDB_BATCH_GET
    virtual void on_batch_get(batch_get_rpc rpc) = 0;
    // RPC_RRDB_RRDB_SORTKEY_COUNT
    virtual void on_sortkey_count(sortkey_count_rpc rpc) = 0;
    // RPC_RRDB_RRDB_TTL
    virtual void on_ttl(ttl_rpc rpc) = 0;
    // RPC_RRDB_RRDB_GET_SCANNER
    virtual void on_get_scanner(get_scanner_rpc rpc) = 0;
    // RPC_RRDB_RRDB_SCAN
    virtual void on_scan(scan_rpc rpc) = 0;
    // RPC_RRDB_RRDB_CLEAR_SCANNER
    virtual void on_clear_scanner(const int64_t &args) = 0;

    static void register_rpc_handlers()
    {
        register_rpc_handler_with_rpc_holder(dsn::apps::RPC_RRDB_RRDB_GET, "get", on_get);
        register_rpc_handler_with_rpc_holder(
            dsn::apps::RPC_RRDB_RRDB_MULTI_GET, "multi_get", on_multi_get);
        register_rpc_handler_with_rpc_holder(
            dsn::apps::RPC_RRDB_RRDB_BATCH_GET, "batch_get", on_batch_get);
        register_rpc_handler_with_rpc_holder(
            dsn::apps::RPC_RRDB_RRDB_SORTKEY_COUNT, "sortkey_count", on_sortkey_count);
        register_rpc_handler_with_rpc_holder(dsn::apps::RPC_RRDB_RRDB_TTL, "ttl", on_ttl);
        register_rpc_handler_with_rpc_holder(
            dsn::apps::RPC_RRDB_RRDB_GET_SCANNER, "get_scanner", on_get_scanner);
        register_rpc_handler_with_rpc_holder(dsn::apps::RPC_RRDB_RRDB_SCAN, "scan", on_scan);
        register_async_rpc_handler(
            dsn::apps::RPC_RRDB_RRDB_CLEAR_SCANNER, "clear_scanner", on_clear_scanner);
    }

private:
    static void on_get(pegasus_read_service *svc, get_rpc rpc) { svc->on_get(rpc); }
    static void on_multi_get(pegasus_read_service *svc, multi_get_rpc rpc)
    {
        svc->on_multi_get(rpc);
    }
    static void on_batch_get(pegasus_read_service *svc, batch_get_rpc rpc)
    {
        svc->on_batch_get(rpc);
    }
    static void on_sortkey_count(pegasus_read_service *svc, sortkey_count_rpc rpc)
    {
        svc->on_sortkey_count(rpc);
    }
    static void on_ttl(pegasus_read_service *svc, ttl_rpc rpc) { svc->on_ttl(rpc); }
    static void on_get_scanner(pegasus_read_service *svc, get_scanner_rpc rpc)
    {
        svc->on_get_scanner(rpc);
    }
    static void on_scan(pegasus_read_service *svc, scan_rpc rpc) { svc->on_scan(rpc); }
    static void on_clear_scanner(pegasus_read_service *svc, const int64_t &args)
    {
        svc->on_clear_scanner(args);
    }
};
} // namespace server
} // namespace pegasus
