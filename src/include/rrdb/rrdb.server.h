#pragma once
#include <iostream>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/replication/storage_serverlet.h>
#include <rrdb/rrdb.code.definition.h>

namespace dsn {
namespace apps {
class rrdb_service : public replication::replication_app_base,
                     public replication::storage_serverlet<rrdb_service>
{
public:
    rrdb_service(replication::replica *r) : replication::replication_app_base(r) {}
    virtual ~rrdb_service() {}
    virtual int on_request(dsn::message_ex *request) override
    {
        handle_request(request);
        return 0;
    }

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
    // RPC_RRDB_RRDB_INCR
    virtual void on_incr(const incr_request &args, ::dsn::rpc_replier<incr_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_INCR ... (not implemented) " << std::endl;
        incr_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_CHECK_AND_SET
    virtual void on_check_and_set(const check_and_set_request &args,
                                  ::dsn::rpc_replier<check_and_set_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_CHECK_AND_SET ... (not implemented) " << std::endl;
        check_and_set_response resp;
        reply(resp);
    }
    // RPC_RRDB_RRDB_CHECK_AND_MUTATE
    virtual void on_check_and_mutate(const check_and_mutate_request &args,
                                     ::dsn::rpc_replier<check_and_mutate_response> &reply)
    {
        std::cout << "... exec RPC_RRDB_RRDB_CHECK_AND_MUTATE ... (not implemented) " << std::endl;
        check_and_mutate_response resp;
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

    static void register_rpc_handlers()
    {
        register_async_rpc_handler(RPC_RRDB_RRDB_PUT, "put", on_put);
        register_async_rpc_handler(RPC_RRDB_RRDB_MULTI_PUT, "multi_put", on_multi_put);
        register_async_rpc_handler(RPC_RRDB_RRDB_REMOVE, "remove", on_multi_remove);
        register_async_rpc_handler(RPC_RRDB_RRDB_INCR, "incr", on_incr);
        register_async_rpc_handler(RPC_RRDB_RRDB_CHECK_AND_SET, "check_and_set", on_check_and_set);
        register_async_rpc_handler(
            RPC_RRDB_RRDB_CHECK_AND_MUTATE, "check_and_mutate", on_check_and_mutate);
        register_async_rpc_handler(RPC_RRDB_RRDB_GET, "get", on_get);
        register_async_rpc_handler(RPC_RRDB_RRDB_MULTI_GET, "multi_get", on_multi_get);
        register_async_rpc_handler(RPC_RRDB_RRDB_SORTKEY_COUNT, "sortkey_count", on_sortkey_count);
        register_async_rpc_handler(RPC_RRDB_RRDB_TTL, "ttl", on_ttl);
        register_async_rpc_handler(RPC_RRDB_RRDB_GET_SCANNER, "get_scanner", on_get_scanner);
        register_async_rpc_handler(RPC_RRDB_RRDB_SCAN, "scan", on_scan);
        register_async_rpc_handler(RPC_RRDB_RRDB_CLEAR_SCANNER, "clear_scanner", on_clear_scanner);
    }

private:
    static void on_put(rrdb_service *svc,
                       const update_request &args,
                       ::dsn::rpc_replier<update_response> &reply)
    {
        svc->on_put(args, reply);
    }
    static void on_multi_put(rrdb_service *svc,
                             const multi_put_request &args,
                             ::dsn::rpc_replier<update_response> &reply)
    {
        svc->on_multi_put(args, reply);
    }
    static void on_remove(rrdb_service *svc,
                          const ::dsn::blob &args,
                          ::dsn::rpc_replier<update_response> &reply)
    {
        svc->on_remove(args, reply);
    }
    static void on_multi_remove(rrdb_service *svc,
                                const multi_remove_request &args,
                                ::dsn::rpc_replier<multi_remove_response> &reply)
    {
        svc->on_multi_remove(args, reply);
    }
    static void
    on_incr(rrdb_service *svc, const incr_request &args, ::dsn::rpc_replier<incr_response> &reply)
    {
        svc->on_incr(args, reply);
    }
    static void on_check_and_set(rrdb_service *svc,
                                 const check_and_set_request &args,
                                 ::dsn::rpc_replier<check_and_set_response> &reply)
    {
        svc->on_check_and_set(args, reply);
    }
    static void on_check_and_mutate(rrdb_service *svc,
                                    const check_and_mutate_request &args,
                                    ::dsn::rpc_replier<check_and_mutate_response> &reply)
    {
        svc->on_check_and_mutate(args, reply);
    }
    static void
    on_get(rrdb_service *svc, const ::dsn::blob &args, ::dsn::rpc_replier<read_response> &reply)
    {
        svc->on_get(args, reply);
    }
    static void on_multi_get(rrdb_service *svc,
                             const multi_get_request &args,
                             ::dsn::rpc_replier<multi_get_response> &reply)
    {
        svc->on_multi_get(args, reply);
    }
    static void on_sortkey_count(rrdb_service *svc,
                                 const ::dsn::blob &args,
                                 ::dsn::rpc_replier<count_response> &reply)
    {
        svc->on_sortkey_count(args, reply);
    }
    static void
    on_ttl(rrdb_service *svc, const ::dsn::blob &args, ::dsn::rpc_replier<ttl_response> &reply)
    {
        svc->on_ttl(args, reply);
    }
    static void on_get_scanner(rrdb_service *svc,
                               const get_scanner_request &args,
                               ::dsn::rpc_replier<scan_response> &reply)
    {
        svc->on_get_scanner(args, reply);
    }
    static void
    on_scan(rrdb_service *svc, const scan_request &args, ::dsn::rpc_replier<scan_response> &reply)
    {
        svc->on_scan(args, reply);
    }
    static void on_clear_scanner(rrdb_service *svc, const int64_t &args)
    {
        svc->on_clear_scanner(args);
    }
};
} // namespace apps
} // namespace dsn
