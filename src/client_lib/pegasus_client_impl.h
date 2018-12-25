// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <pegasus/client.h>
#include <rrdb/rrdb.client.h>
#include <dsn/tool-api/zlocks.h>
#include "base/pegasus_key_schema.h"
#include "base/pegasus_utils.h"

namespace pegasus {
namespace client {

class pegasus_client_impl : public pegasus_client
{
public:
    pegasus_client_impl(const char *cluster_name, const char *app_name);
    virtual ~pegasus_client_impl();

    virtual const char *get_cluster_name() const override;

    virtual const char *get_app_name() const override;

    virtual int set(const std::string &hashkey,
                    const std::string &sortkey,
                    const std::string &value,
                    int timeout_milliseconds = 5000,
                    int ttl_seconds = 0,
                    internal_info *info = nullptr) override;

    virtual void async_set(const std::string &hashkey,
                           const std::string &sortkey,
                           const std::string &value,
                           async_set_callback_t &&callback = nullptr,
                           int timeout_milliseconds = 5000,
                           int ttl_seconds = 0) override;

    virtual int multi_set(const std::string &hashkey,
                          const std::map<std::string, std::string> &kvs,
                          int timeout_milliseconds = 5000,
                          int ttl_seconds = 0,
                          internal_info *info = nullptr) override;

    virtual void async_multi_set(const std::string &hashkey,
                                 const std::map<std::string, std::string> &kvs,
                                 async_multi_set_callback_t &&callback = nullptr,
                                 int timeout_milliseconds = 5000,
                                 int ttl_seconds = 0) override;

    virtual int get(const std::string &hashkey,
                    const std::string &sortkey,
                    std::string &value,
                    int timeout_milliseconds = 5000,
                    internal_info *info = nullptr) override;

    virtual void async_get(const std::string &hashkey,
                           const std::string &sortkey,
                           async_get_callback_t &&callback = nullptr,
                           int timeout_milliseconds = 5000) override;

    virtual int multi_get(const std::string &hashkey,
                          const std::set<std::string> &sortkeys,
                          std::map<std::string, std::string> &values,
                          int max_fetch_count = 100,
                          int max_fetch_size = 1000000,
                          int timeout_milliseconds = 5000,
                          internal_info *info = nullptr) override;

    virtual void async_multi_get(const std::string &hashkey,
                                 const std::set<std::string> &sortkeys,
                                 async_multi_get_callback_t &&callback = nullptr,
                                 int max_fetch_count = 100,
                                 int max_fetch_size = 1000000,
                                 int timeout_milliseconds = 5000) override;

    virtual int multi_get(const std::string &hashkey,
                          const std::string &start_sortkey,
                          const std::string &stop_sortkey,
                          const multi_get_options &options,
                          std::map<std::string, std::string> &values,
                          int max_fetch_count = 100,
                          int max_fetch_size = 1000000,
                          int timeout_milliseconds = 5000,
                          internal_info *info = nullptr) override;

    virtual void async_multi_get(const std::string &hashkey,
                                 const std::string &start_sortkey,
                                 const std::string &stop_sortkey,
                                 const multi_get_options &options,
                                 async_multi_get_callback_t &&callback = nullptr,
                                 int max_fetch_count = 100,
                                 int max_fetch_size = 1000000,
                                 int timeout_milliseconds = 5000) override;

    virtual int multi_get_sortkeys(const std::string &hashkey,
                                   std::set<std::string> &sortkeys,
                                   int max_fetch_count = 100,
                                   int max_fetch_size = 1000000,
                                   int timeout_milliseconds = 5000,
                                   internal_info *info = nullptr) override;

    virtual void async_multi_get_sortkeys(const std::string &hashkey,
                                          async_multi_get_sortkeys_callback_t &&callback = nullptr,
                                          int max_fetch_count = 100,
                                          int max_fetch_size = 1000000,
                                          int timeout_milliseconds = 5000) override;

    virtual int exist(const std::string &hashkey,
                      const std::string &sortkey,
                      int timeout_milliseconds = 5000,
                      internal_info *info = nullptr) override;

    virtual int sortkey_count(const std::string &hashkey,
                              int64_t &count,
                              int timeout_milliseconds = 5000,
                              internal_info *info = nullptr) override;

    virtual int del(const std::string &hashkey,
                    const std::string &sortkey,
                    int timeout_milliseconds = 5000,
                    internal_info *info = nullptr) override;

    virtual void async_del(const std::string &hashkey,
                           const std::string &sortkey,
                           async_del_callback_t &&callback = nullptr,
                           int timeout_milliseconds = 5000) override;

    virtual int multi_del(const std::string &hashkey,
                          const std::set<std::string> &sortkeys,
                          int64_t &deleted_count,
                          int timeout_milliseconds = 5000,
                          internal_info *info = nullptr) override;

    virtual void async_multi_del(const std::string &hashkey,
                                 const std::set<std::string> &sortkeys,
                                 async_multi_del_callback_t &&callback = nullptr,
                                 int timeout_milliseconds = 5000) override;

    virtual int incr(const std::string &hashkey,
                     const std::string &sortkey,
                     int64_t increment,
                     int64_t &new_value,
                     int timeout_milliseconds = 5000,
                     int ttl_seconds = 0,
                     internal_info *info = nullptr) override;

    virtual void async_incr(const std::string &hashkey,
                            const std::string &sortkey,
                            int64_t increment,
                            async_incr_callback_t &&callback = nullptr,
                            int timeout_milliseconds = 5000,
                            int ttl_seconds = 0) override;

    virtual int check_and_set(const std::string &hash_key,
                              const std::string &check_sort_key,
                              cas_check_type check_type,
                              const std::string &check_operand,
                              const std::string &set_sort_key,
                              const std::string &set_value,
                              const check_and_set_options &options,
                              check_and_set_results &results,
                              int timeout_milliseconds = 5000,
                              internal_info *info = nullptr) override;

    virtual void async_check_and_set(const std::string &hash_key,
                                     const std::string &check_sort_key,
                                     cas_check_type check_type,
                                     const std::string &check_operand,
                                     const std::string &set_sort_key,
                                     const std::string &set_value,
                                     const check_and_set_options &options,
                                     async_check_and_set_callback_t &&callback = nullptr,
                                     int timeout_milliseconds = 5000) override;

    virtual int check_and_mutate(const std::string &hash_key,
                                 const std::string &check_sort_key,
                                 cas_check_type check_type,
                                 const std::string &check_operand,
                                 const mutations &mutations,
                                 const check_and_mutate_options &options,
                                 check_and_mutate_results &results,
                                 int timeout_milliseconds = 5000,
                                 internal_info *info = nullptr) override;

    virtual void async_check_and_mutate(const std::string &hash_key,
                                        const std::string &check_sort_key,
                                        cas_check_type check_type,
                                        const std::string &check_operand,
                                        const mutations &mutations,
                                        const check_and_mutate_options &options,
                                        async_check_and_mutate_callback_t &&callback = nullptr,
                                        int timeout_milliseconds = 5000) override;

    virtual int ttl(const std::string &hashkey,
                    const std::string &sortkey,
                    int &ttl_seconds,
                    int timeout_milliseconds = 5000,
                    internal_info *info = nullptr) override;

    virtual int get_scanner(const std::string &hashkey,
                            const std::string &start_sortkey,
                            const std::string &stop_sortkey,
                            const scan_options &options,
                            pegasus_scanner *&scanner) override;

    virtual void async_get_scanner(const std::string &hashkey,
                                   const std::string &start_sortkey,
                                   const std::string &stop_sortkey,
                                   const scan_options &options,
                                   async_get_scanner_callback_t &&callback) override;

    virtual int get_unordered_scanners(int max_split_count,
                                       const scan_options &options,
                                       std::vector<pegasus_scanner *> &scanners) override;

    virtual void
    async_get_unordered_scanners(int max_split_count,
                                 const scan_options &options,
                                 async_get_unordered_scanners_callback_t &&callback) override;

    virtual const char *get_error_string(int error_code) const override;

    static void init_error();

    class pegasus_scanner_impl : public pegasus_scanner
    {
    public:
        int next(std::string &hashkey,
                 std::string &sortkey,
                 std::string &value,
                 internal_info *info = nullptr) override;

        void async_next(async_scan_next_callback_t &&) override;

        bool safe_destructible() const override;

        pegasus_scanner_wrapper get_smart_wrapper() override;

        ~pegasus_scanner_impl() override;

        pegasus_scanner_impl(::dsn::apps::rrdb_client *client,
                             std::vector<uint64_t> &&hash,
                             const scan_options &options);
        pegasus_scanner_impl(::dsn::apps::rrdb_client *client,
                             std::vector<uint64_t> &&hash,
                             const scan_options &options,
                             const ::dsn::blob &start_key,
                             const ::dsn::blob &stop_key);

    private:
        ::dsn::apps::rrdb_client *_client;
        ::dsn::blob _start_key;
        ::dsn::blob _stop_key;
        scan_options _options;
        std::vector<uint64_t> _splits_hash;

        uint64_t _hash;
        std::vector<::dsn::apps::key_value> _kvs;
        internal_info _info;
        int32_t _p;

        int64_t _context;
        mutable ::dsn::zlock _lock;
        std::list<async_scan_next_callback_t> _queue;
        volatile bool _rpc_started;

        void _async_next_internal();
        void _start_scan();
        void _next_batch();
        void _on_scan_response(::dsn::error_code, dsn::message_ex *, dsn::message_ex *);
        void _split_reset();

    private:
        static const char _holder[];
        static const ::dsn::blob _min;
        static const ::dsn::blob _max;
    };

private:
    class pegasus_scanner_impl_wrapper : public abstract_pegasus_scanner
    {
        std::shared_ptr<pegasus_scanner> _p;

    public:
        pegasus_scanner_impl_wrapper(pegasus_scanner *p) : _p(p) {}

        void async_next(async_scan_next_callback_t &&callback) override;

        int next(std::string &hashkey,
                 std::string &sortkey,
                 std::string &value,
                 internal_info *info) override
        {
            return _p->next(hashkey, sortkey, value, info);
        }
    };

    static int get_client_error(int server_error);
    static int get_rocksdb_server_error(int rocskdb_error);

private:
    std::string _cluster_name;
    std::string _app_name;
    ::dsn::rpc_address _meta_server;
    ::dsn::apps::rrdb_client *_client;

    ///
    /// \brief _client_error_to_string
    /// store int to string for client call get_error_string()
    ///
    static std::unordered_map<int, std::string> _client_error_to_string;

    ///
    /// \brief _server_error_to_client
    /// translate server error to client, it will find from a map<int, int>
    /// the map is initialized in init_error() which will be called on client lib initailization.
    ///
    static std::unordered_map<int, int> _server_error_to_client;
};
} // namespace client
} // namespace pegasus
