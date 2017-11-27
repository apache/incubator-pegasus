#pragma once
#include <rrdb/rrdb.code.definition.h>
#include <iostream>

namespace dsn {
namespace apps {
class rrdb_service : public ::dsn::serverlet<rrdb_service>
{
public:
    rrdb_service() : ::dsn::serverlet<rrdb_service>("rrdb") {}
    virtual ~rrdb_service() {}

protected:
    // all service handlers to be implemented further
    // RPC_RRDB_RRDB_PUT
    virtual void on_put(const update_request &args, ::dsn::rpc_replier<update_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_PUT ... (not implemented) " << std::endl;
        update_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_MULTI_PUT
    virtual void on_multi_put(const multi_put_request &args,
                              ::dsn::rpc_replier<update_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_MULTI_PUT ... (not implemented) " << std::endl;
        update_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_REMOVE
    virtual void on_remove(const ::dsn::blob &args, ::dsn::rpc_replier<update_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_REMOVE ... (not implemented) " << std::endl;
        update_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_MULTI_REMOVE
    virtual void on_multi_remove(const multi_remove_request &args,
                                 ::dsn::rpc_replier<multi_remove_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_MULTI_REMOVE ... (not implemented) " << std::endl;
        multi_remove_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_GET
    virtual void on_get(const ::dsn::blob &args, ::dsn::rpc_replier<read_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_GET ... (not implemented) " << std::endl;
        read_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_MULTI_GET
    virtual void on_multi_get(const multi_get_request &args,
                              ::dsn::rpc_replier<multi_get_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_MULTI_GET ... (not implemented) " << std::endl;
        multi_get_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_SORTKEY_COUNT
    virtual void on_sortkey_count(const ::dsn::blob &args,
                                  ::dsn::rpc_replier<count_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_SORTKEY_COUNT ... (not implemented) " << std::endl;
        count_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_TTL
    virtual void on_ttl(const ::dsn::blob &args, ::dsn::rpc_replier<ttl_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_TTL ... (not implemented) " << std::endl;
        ttl_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_GET_SCANNER
    virtual void on_get_scanner(const get_scanner_request &args,
                                ::dsn::rpc_replier<scan_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_GET_SCANNER ... (not implemented) " << std::endl;
        scan_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_SCAN
    virtual void on_scan(const scan_request &args, ::dsn::rpc_replier<scan_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_SCAN ... (not implemented) " << std::endl;
        scan_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_CLEAR_SCANNER
    virtual void on_clear_scanner(const int64_t &args)
    {
        std::cout << "... exec RPC_RRDB_RRDB_CLEAR_SCANNER ... (not implemented) " << std::endl;
    }

public:
    void open_service(dsn_gpid gpid)
    {
        this->register_async_rpc_handler(RPC_RRDB_RRDB_PUT, "put", &rrdb_service::on_put, gpid);
        this->register_async_rpc_handler(
            RPC_RRDB_RRDB_MULTI_PUT, "multi_put", &rrdb_service::on_multi_put, gpid);
        this->register_async_rpc_handler(
            RPC_RRDB_RRDB_REMOVE, "remove", &rrdb_service::on_remove, gpid);
        this->register_async_rpc_handler(
            RPC_RRDB_RRDB_MULTI_REMOVE, "multi_remove", &rrdb_service::on_multi_remove, gpid);
        this->register_async_rpc_handler(RPC_RRDB_RRDB_GET, "get", &rrdb_service::on_get, gpid);
        this->register_async_rpc_handler(
            RPC_RRDB_RRDB_MULTI_GET, "multi_get", &rrdb_service::on_multi_get, gpid);
        this->register_async_rpc_handler(
            RPC_RRDB_RRDB_SORTKEY_COUNT, "sortkey_count", &rrdb_service::on_sortkey_count, gpid);
        this->register_async_rpc_handler(RPC_RRDB_RRDB_TTL, "ttl", &rrdb_service::on_ttl, gpid);
        this->register_async_rpc_handler(
            RPC_RRDB_RRDB_GET_SCANNER, "get_scanner", &rrdb_service::on_get_scanner, gpid);
        this->register_async_rpc_handler(RPC_RRDB_RRDB_SCAN, "scan", &rrdb_service::on_scan, gpid);
        this->register_rpc_handler(
            RPC_RRDB_RRDB_CLEAR_SCANNER, "clear_scanner", &rrdb_service::on_clear_scanner, gpid);
    }

    void close_service(dsn_gpid gpid)
    {
        this->unregister_rpc_handler(RPC_RRDB_RRDB_PUT, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_MULTI_PUT, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_REMOVE, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_MULTI_REMOVE, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_GET, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_MULTI_GET, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_SORTKEY_COUNT, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_TTL, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_GET_SCANNER, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_SCAN, gpid);
        this->unregister_rpc_handler(RPC_RRDB_RRDB_CLEAR_SCANNER, gpid);
    }
};
}
}