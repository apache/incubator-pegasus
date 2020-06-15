#pragma once
#include <iostream>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/replication/storage_serverlet.h>
#include <rrdb/rrdb.code.definition.h>

namespace pegasus {
namespace server {
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
    virtual void on_get(const ::dsn::blob &args,
                        ::dsn::rpc_replier<dsn::apps::read_response> &reply) = 0;
    // RPC_RRDB_RRDB_MULTI_GET
    virtual void on_multi_get(const dsn::apps::multi_get_request &args,
                              ::dsn::rpc_replier<dsn::apps::multi_get_response> &reply) = 0;
    // RPC_RRDB_RRDB_SORTKEY_COUNT
    virtual void on_sortkey_count(const ::dsn::blob &args,
                                  ::dsn::rpc_replier<dsn::apps::count_response> &reply) = 0;
    // RPC_RRDB_RRDB_TTL
    virtual void on_ttl(const ::dsn::blob &args,
                        ::dsn::rpc_replier<dsn::apps::ttl_response> &reply) = 0;
    // RPC_RRDB_RRDB_GET_SCANNER
    virtual void on_get_scanner(const dsn::apps::get_scanner_request &args,
                                ::dsn::rpc_replier<dsn::apps::scan_response> &reply) = 0;
    // RPC_RRDB_RRDB_SCAN
    virtual void on_scan(const dsn::apps::scan_request &args,
                         ::dsn::rpc_replier<dsn::apps::scan_response> &reply) = 0;
    // RPC_RRDB_RRDB_CLEAR_SCANNER
    virtual void on_clear_scanner(const int64_t &args) = 0;
    // RPC_DETECT_HOTKEY
    virtual void
    on_detect_hotkey(const ::dsn::apps::hotkey_detect_request &args,
                     ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> &reply) = 0;

    static void register_rpc_handlers()
    {
        register_async_rpc_handler(dsn::apps::RPC_RRDB_RRDB_GET, "get", on_get);
        register_async_rpc_handler(dsn::apps::RPC_RRDB_RRDB_MULTI_GET, "multi_get", on_multi_get);
        register_async_rpc_handler(
            dsn::apps::RPC_RRDB_RRDB_SORTKEY_COUNT, "sortkey_count", on_sortkey_count);
        register_async_rpc_handler(dsn::apps::RPC_RRDB_RRDB_TTL, "ttl", on_ttl);
        register_async_rpc_handler(
            dsn::apps::RPC_RRDB_RRDB_GET_SCANNER, "get_scanner", on_get_scanner);
        register_async_rpc_handler(dsn::apps::RPC_RRDB_RRDB_SCAN, "scan", on_scan);
        register_async_rpc_handler(
            dsn::apps::RPC_RRDB_RRDB_CLEAR_SCANNER, "clear_scanner", on_clear_scanner);
        register_async_rpc_handler(RPC_DETECT_HOTKEY, "detect_hotkey", on_detect_hotkey);
    }

private:
    static void on_get(pegasus_read_service *svc,
                       const ::dsn::blob &args,
                       ::dsn::rpc_replier<dsn::apps::read_response> &reply)
    {
        svc->on_get(args, reply);
    }
    static void on_multi_get(pegasus_read_service *svc,
                             const dsn::apps::multi_get_request &args,
                             ::dsn::rpc_replier<dsn::apps::multi_get_response> &reply)
    {
        svc->on_multi_get(args, reply);
    }
    static void on_sortkey_count(pegasus_read_service *svc,
                                 const ::dsn::blob &args,
                                 ::dsn::rpc_replier<dsn::apps::count_response> &reply)
    {
        svc->on_sortkey_count(args, reply);
    }
    static void on_ttl(pegasus_read_service *svc,
                       const ::dsn::blob &args,
                       ::dsn::rpc_replier<dsn::apps::ttl_response> &reply)
    {
        svc->on_ttl(args, reply);
    }
    static void on_get_scanner(pegasus_read_service *svc,
                               const dsn::apps::get_scanner_request &args,
                               ::dsn::rpc_replier<dsn::apps::scan_response> &reply)
    {
        svc->on_get_scanner(args, reply);
    }
    static void on_scan(pegasus_read_service *svc,
                        const dsn::apps::scan_request &args,
                        ::dsn::rpc_replier<dsn::apps::scan_response> &reply)
    {
        svc->on_scan(args, reply);
    }
    static void on_clear_scanner(pegasus_read_service *svc, const int64_t &args)
    {
        svc->on_clear_scanner(args);
    }
    static void on_detect_hotkey(pegasus_read_service *svc,
                                 const ::dsn::apps::hotkey_detect_request &args,
                                 ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> &reply)
    {
        svc->on_detect_hotkey(args, reply);
    }
};
} // namespace server
} // namespace pegasus
