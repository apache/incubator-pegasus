/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cctype>
#include <algorithm>
#include <string>
#include <stdint.h>

#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/group_address.h"
#include "common/replication_other_types.h"
#include "common/serialization_helper/dsn.layer2_types.h"
#include <rrdb/rrdb.code.definition.h>
#include <pegasus/error.h>
#include "pegasus_client_impl.h"
#include "base/pegasus_const.h"

using namespace ::dsn;

namespace pegasus {
namespace client {

#define ROCSKDB_ERROR_START -1000

std::unordered_map<int, std::string> pegasus_client_impl::_client_error_to_string;
std::unordered_map<int, int> pegasus_client_impl::_server_error_to_client;

pegasus_client_impl::pegasus_client_impl(const char *cluster_name, const char *app_name)
    : _cluster_name(cluster_name), _app_name(app_name)
{
    std::vector<dsn::rpc_address> meta_servers;
    dsn::replication::replica_helper::load_meta_servers(
        meta_servers, PEGASUS_CLUSTER_SECTION_NAME.c_str(), cluster_name);
    CHECK_GT(meta_servers.size(), 0);
    _meta_server.assign_group("meta-servers");
    _meta_server.group_address()->add_list(meta_servers);

    _client = new ::dsn::apps::rrdb_client(cluster_name, meta_servers, app_name);
}

pegasus_client_impl::~pegasus_client_impl() { delete _client; }

const char *pegasus_client_impl::get_cluster_name() const { return _cluster_name.c_str(); }

const char *pegasus_client_impl::get_app_name() const { return _app_name.c_str(); }

int pegasus_client_impl::set(const std::string &hash_key,
                             const std::string &sort_key,
                             const std::string &value,
                             int timeout_milliseconds,
                             int ttl_seconds,
                             internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int err, internal_info &&_info) {
        ret = err;
        if (info != nullptr)
            (*info) = std::move(_info);
        op_completed.notify();
    };
    async_set(hash_key, sort_key, value, std::move(callback), timeout_milliseconds, ttl_seconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_set(const std::string &hash_key,
                                    const std::string &sort_key,
                                    const std::string &value,
                                    async_set_callback_t &&callback,
                                    int timeout_milliseconds,
                                    int ttl_seconds)
{
    // check params
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, internal_info());
        return;
    }
    ::dsn::apps::update_request req;
    pegasus_generate_key(req.key, hash_key, sort_key);
    req.value.assign(value.c_str(), 0, value.size());
    if (ttl_seconds == 0)
        req.expire_ts_seconds = 0;
    else
        req.expire_ts_seconds = ttl_seconds + utils::epoch_now();

    auto partition_hash = pegasus_key_hash(req.key);

    // wrap the user defined callback function, generate a new callback function.
    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        internal_info info;
        ::dsn::apps::update_response response;
        if (err == ::dsn::ERR_OK) {
            ::dsn::unmarshall(resp, response);
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.decree = response.decree;
            info.server = response.server;
        }
        auto ret = get_client_error(
            (err == ::dsn::ERR_OK) ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(info));
    };
    _client->put(req,
                 std::move(new_callback),
                 std::chrono::milliseconds(timeout_milliseconds),
                 partition_hash);
}

int pegasus_client_impl::multi_set(const std::string &hash_key,
                                   const std::map<std::string, std::string> &kvs,
                                   int timeout_milliseconds,
                                   int ttl_seconds,
                                   internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int err, internal_info &&_info) {
        ret = err;
        if (info != nullptr)
            (*info) = std::move(_info);
        op_completed.notify();
    };
    async_multi_set(hash_key, kvs, std::move(callback), timeout_milliseconds, ttl_seconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_multi_set(const std::string &hash_key,
                                          const std::map<std::string, std::string> &kvs,
                                          async_multi_set_callback_t &&callback,
                                          int timeout_milliseconds,
                                          int ttl_seconds)
{
    // check params
    if (hash_key.size() == 0) {
        LOG_ERROR("invalid hash key: hash key should not be empty for multi_set");
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, internal_info());
        return;
    }
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, internal_info());
        return;
    }
    if (kvs.empty()) {
        LOG_ERROR("invalid kvs: kvs should not be empty");
        if (callback != nullptr)
            callback(PERR_INVALID_VALUE, internal_info());
        return;
    }

    ::dsn::apps::multi_put_request req;
    req.hash_key = ::dsn::blob(hash_key.data(), 0, hash_key.size());
    for (auto &kv : kvs) {
        ::dsn::apps::key_value kv_blob;
        kv_blob.key = ::dsn::blob(kv.first.data(), 0, kv.first.size());
        kv_blob.value = ::dsn::blob(kv.second.data(), 0, kv.second.size());
        req.kvs.emplace_back(std::move(kv_blob));
    }
    if (ttl_seconds == 0)
        req.expire_ts_seconds = 0;
    else
        req.expire_ts_seconds = ttl_seconds + utils::epoch_now();

    ::dsn::blob tmp_key;
    pegasus_generate_key(tmp_key, req.hash_key, ::dsn::blob());
    auto partition_hash = pegasus_key_hash(tmp_key);
    // wrap the user-defined-callback-function, generate a new callback function.
    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        internal_info info;
        ::dsn::apps::update_response response;
        if (err == ::dsn::ERR_OK) {
            ::dsn::unmarshall(resp, response);
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.decree = response.decree;
            info.server = response.server;
        }
        auto ret = get_client_error(
            (err == ::dsn::ERR_OK) ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(info));
    };
    _client->multi_put(req,
                       std::move(new_callback),
                       std::chrono::milliseconds(timeout_milliseconds),
                       partition_hash);
}

int pegasus_client_impl::get(const std::string &hash_key,
                             const std::string &sort_key,
                             std::string &value,
                             int timeout_milliseconds,
                             internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int err, std::string &&str, internal_info &&_info) {
        ret = err;
        value = std::move(str);
        if (info != nullptr)
            (*info) = std::move(_info);
        op_completed.notify();
    };
    async_get(hash_key, sort_key, std::move(callback), timeout_milliseconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_get(const std::string &hash_key,
                                    const std::string &sort_key,
                                    async_get_callback_t &&callback,
                                    int timeout_milliseconds)
{
    // check params
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, std::string(), internal_info());
        return;
    }
    ::dsn::blob req;
    pegasus_generate_key(req, hash_key, sort_key);
    auto partition_hash = pegasus_key_hash(req);
    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        std::string value;
        internal_info info;
        dsn::apps::read_response response;
        if (err == ::dsn::ERR_OK) {
            ::dsn::unmarshall(resp, response);
            if (response.error == 0) {
                value.assign(response.value.data(), response.value.length());
            }
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.server = response.server;
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(value), std::move(info));
    };
    _client->get(req,
                 std::move(new_callback),
                 std::chrono::milliseconds(timeout_milliseconds),
                 partition_hash);
}

int pegasus_client_impl::multi_get(const std::string &hash_key,
                                   const std::set<std::string> &sort_keys,
                                   std::map<std::string, std::string> &values,
                                   int max_fetch_count,
                                   int max_fetch_size,
                                   int timeout_milliseconds,
                                   internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback =
        [&](int err, std::map<std::string, std::string> &&_values, internal_info &&_info) {
            ret = err;
            if (info != nullptr)
                (*info) = std::move(_info);
            values = std::move(_values);
            op_completed.notify();
        };
    async_multi_get(hash_key,
                    sort_keys,
                    std::move(callback),
                    max_fetch_count,
                    max_fetch_size,
                    timeout_milliseconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_multi_get(const std::string &hash_key,
                                          const std::set<std::string> &sort_keys,
                                          async_multi_get_callback_t &&callback,
                                          int max_fetch_count,
                                          int max_fetch_size,
                                          int timeout_milliseconds)
{
    // check params
    if (hash_key.size() == 0) {
        LOG_ERROR("invalid hash key: hash key should not be empty");
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, std::map<std::string, std::string>(), internal_info());
        return;
    }
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, std::map<std::string, std::string>(), internal_info());
        return;
    }

    ::dsn::apps::multi_get_request req;
    req.hash_key = ::dsn::blob(hash_key.data(), 0, hash_key.size());
    req.max_kv_count = max_fetch_count;
    req.max_kv_size = max_fetch_size;
    req.start_inclusive = true;
    req.stop_inclusive = false;
    for (auto &sort_key : sort_keys) {
        req.sort_keys.emplace_back(sort_key.data(), 0, sort_key.size());
    }
    ::dsn::blob tmp_key;
    pegasus_generate_key(tmp_key, req.hash_key, ::dsn::blob());
    auto partition_hash = pegasus_key_hash(tmp_key);
    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        std::map<std::string, std::string> values;
        internal_info info;
        ::dsn::apps::multi_get_response response;
        if (err == ::dsn::ERR_OK) {
            ::unmarshall(resp, response);
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.server = response.server;
            for (auto &kv : response.kvs)
                values.emplace(std::string(kv.key.data(), kv.key.length()),
                               std::string(kv.value.data(), kv.value.length()));
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(values), std::move(info));
    };
    _client->multi_get(req,
                       std::move(new_callback),
                       std::chrono::milliseconds(timeout_milliseconds),
                       partition_hash);
}

int pegasus_client_impl::multi_get(const std::string &hash_key,
                                   const std::string &start_sortkey,
                                   const std::string &stop_sortkey,
                                   const multi_get_options &options,
                                   std::map<std::string, std::string> &values,
                                   int max_fetch_count,
                                   int max_fetch_size,
                                   int timeout_milliseconds,
                                   internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback =
        [&](int err, std::map<std::string, std::string> &&_values, internal_info &&_info) {
            ret = err;
            if (info != nullptr)
                (*info) = std::move(_info);
            values = std::move(_values);
            op_completed.notify();
        };
    async_multi_get(hash_key,
                    start_sortkey,
                    stop_sortkey,
                    options,
                    std::move(callback),
                    max_fetch_count,
                    max_fetch_size,
                    timeout_milliseconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_multi_get(const std::string &hash_key,
                                          const std::string &start_sortkey,
                                          const std::string &stop_sortkey,
                                          const multi_get_options &options,
                                          async_multi_get_callback_t &&callback,
                                          int max_fetch_count,
                                          int max_fetch_size,
                                          int timeout_milliseconds)
{
    // check params
    if (hash_key.size() == 0) {
        LOG_ERROR("invalid hash key: hash key should not be empty");
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, std::map<std::string, std::string>(), internal_info());
        return;
    }
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, std::map<std::string, std::string>(), internal_info());
        return;
    }

    ::dsn::apps::multi_get_request req;
    req.hash_key = ::dsn::blob(hash_key.data(), 0, hash_key.size());
    req.start_sortkey = ::dsn::blob(start_sortkey.data(), 0, start_sortkey.size());
    req.stop_sortkey = ::dsn::blob(stop_sortkey.data(), 0, stop_sortkey.size());
    req.start_inclusive = options.start_inclusive;
    req.stop_inclusive = options.stop_inclusive;
    req.max_kv_count = max_fetch_count;
    req.max_kv_size = max_fetch_size;
    req.no_value = options.no_value;
    req.reverse = options.reverse;
    req.sort_key_filter_type = (dsn::apps::filter_type::type)options.sort_key_filter_type;
    req.sort_key_filter_pattern = ::dsn::blob(
        options.sort_key_filter_pattern.data(), 0, options.sort_key_filter_pattern.size());
    ::dsn::blob tmp_key;
    pegasus_generate_key(tmp_key, req.hash_key, ::dsn::blob());
    auto partition_hash = pegasus_key_hash(tmp_key);
    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        std::map<std::string, std::string> values;
        internal_info info;
        ::dsn::apps::multi_get_response response;
        if (err == ::dsn::ERR_OK) {
            ::unmarshall(resp, response);
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.server = response.server;
            for (auto &kv : response.kvs)
                values.emplace(std::string(kv.key.data(), kv.key.length()),
                               std::string(kv.value.data(), kv.value.length()));
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(values), std::move(info));
    };
    _client->multi_get(req,
                       std::move(new_callback),
                       std::chrono::milliseconds(timeout_milliseconds),
                       partition_hash);
}

int pegasus_client_impl::multi_get_sortkeys(const std::string &hash_key,
                                            std::set<std::string> &sort_keys,
                                            int max_fetch_count,
                                            int max_fetch_size,
                                            int timeout_milliseconds,
                                            internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int err, std::set<std::string> &&_sort_keys, internal_info &&_info) {
        ret = err;
        if (info != nullptr)
            (*info) = std::move(_info);
        sort_keys = std::move(_sort_keys);
        op_completed.notify();
    };
    async_multi_get_sortkeys(
        hash_key, std::move(callback), max_fetch_count, max_fetch_size, timeout_milliseconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_multi_get_sortkeys(const std::string &hash_key,
                                                   async_multi_get_sortkeys_callback_t &&callback,
                                                   int max_fetch_count,
                                                   int max_fetch_size,
                                                   int timeout_milliseconds)
{
    // check params
    if (hash_key.size() == 0) {
        LOG_ERROR("invalid hash key: hash key should not be empty for multi_get_sortkeys");
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, std::set<std::string>(), internal_info());
        return;
    }
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, std::set<std::string>(), internal_info());
        return;
    }

    ::dsn::apps::multi_get_request req;
    req.hash_key = ::dsn::blob(hash_key.data(), 0, hash_key.size());
    req.max_kv_count = max_fetch_count;
    req.max_kv_size = max_fetch_size;
    req.no_value = true;
    ::dsn::blob tmp_key;
    pegasus_generate_key(tmp_key, req.hash_key, ::dsn::blob());
    auto partition_hash = pegasus_key_hash(tmp_key);
    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        std::set<std::string> sort_keys;
        internal_info info;
        ::dsn::apps::multi_get_response response;
        if (err == ::dsn::ERR_OK) {
            ::unmarshall(resp, response);
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.server = response.server;
            for (auto &kv : response.kvs)
                sort_keys.insert(std::string(kv.key.data(), kv.key.length()));
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(sort_keys), std::move(info));
    };
    _client->multi_get(req,
                       std::move(new_callback),
                       std::chrono::milliseconds(timeout_milliseconds),
                       partition_hash);
}

int pegasus_client_impl::exist(const std::string &hash_key,
                               const std::string &sort_key,
                               int timeout_milliseconds,
                               internal_info *info)
{
    int ttl_seconds;
    return ttl(hash_key, sort_key, ttl_seconds, timeout_milliseconds, info);
}

int pegasus_client_impl::sortkey_count(const std::string &hash_key,
                                       int64_t &count,
                                       int timeout_milliseconds,
                                       internal_info *info)
{
    // check params
    if (hash_key.size() == 0) {
        LOG_ERROR("invalid hash key: hash key should not be empty for sortkey_count");
        return PERR_INVALID_HASH_KEY;
    }
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        return PERR_INVALID_HASH_KEY;
    }

    ::dsn::blob tmp_key;
    pegasus_generate_key(tmp_key, hash_key, std::string());
    auto partition_hash = pegasus_key_hash(tmp_key);
    auto pr = _client->sortkey_count_sync(::dsn::blob(hash_key.data(), 0, hash_key.length()),
                                          std::chrono::milliseconds(timeout_milliseconds),
                                          partition_hash);
    if (pr.first == ERR_OK && pr.second.error == 0) {
        count = pr.second.count;
    }
    if (info != nullptr) {
        if (pr.first == ERR_OK) {
            info->app_id = pr.second.app_id;
            info->partition_index = pr.second.partition_index;
            info->decree = -1;
            info->server = pr.second.server;
        } else {
            info->app_id = -1;
            info->partition_index = -1;
            info->decree = -1;
        }
    }
    return get_client_error(pr.first == ERR_OK ? get_rocksdb_server_error(pr.second.error)
                                               : int(pr.first));
}

int pegasus_client_impl::del(const std::string &hash_key,
                             const std::string &sort_key,
                             int timeout_milliseconds,
                             internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int err, internal_info &&_info) {
        ret = err;
        if (info != nullptr)
            (*info) = std::move(_info);
        op_completed.notify();
    };
    async_del(hash_key, sort_key, std::move(callback), timeout_milliseconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_del(const std::string &hash_key,
                                    const std::string &sort_key,
                                    async_del_callback_t &&callback,
                                    int timeout_milliseconds)
{
    // check params
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, internal_info());
        return;
    }

    ::dsn::blob req;
    pegasus_generate_key(req, hash_key, sort_key);
    auto partition_hash = pegasus_key_hash(req);

    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        ::dsn::apps::update_response response;
        internal_info info;
        if (err == ::dsn::ERR_OK) {
            ::dsn::unmarshall(resp, response);
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.decree = response.decree;
            info.server = response.server;
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(info));
    };
    _client->remove(req,
                    std::move(new_callback),
                    std::chrono::milliseconds(timeout_milliseconds),
                    partition_hash);
}

int pegasus_client_impl::multi_del(const std::string &hash_key,
                                   const std::set<std::string> &sort_keys,
                                   int64_t &deleted_count,
                                   int timeout_milliseconds,
                                   internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int err, int64_t _deleted_count, internal_info &&_info) {
        ret = err;
        deleted_count = _deleted_count;
        if (info != nullptr)
            (*info) = std::move(_info);
        op_completed.notify();
    };
    async_multi_del(hash_key, sort_keys, std::move(callback), timeout_milliseconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_multi_del(const std::string &hash_key,
                                          const std::set<std::string> &sort_keys,
                                          async_multi_del_callback_t &&callback,
                                          int timeout_milliseconds)
{
    // check params
    if (hash_key.size() == 0) {
        LOG_ERROR("invalid hash key: hash key should not be empty for multi_del");
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, 0, internal_info());
        return;
    }
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, 0, internal_info());
        return;
    }
    if (sort_keys.empty()) {
        LOG_ERROR("invalid sort keys: should not be empty");
        if (callback != nullptr)
            callback(PERR_INVALID_VALUE, 0, internal_info());
        return;
    }

    ::dsn::apps::multi_remove_request req;
    req.hash_key = ::dsn::blob(hash_key.data(), 0, hash_key.size());
    for (auto &sort_key : sort_keys) {
        req.sort_keys.emplace_back(sort_key.data(), 0, sort_key.size());
    }

    ::dsn::blob tmp_key;
    pegasus_generate_key(tmp_key, req.hash_key, ::dsn::blob());
    auto partition_hash = pegasus_key_hash(tmp_key);

    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        ::dsn::apps::multi_remove_response response;
        internal_info info;
        int64_t deleted_count = 0;
        if (err == ::dsn::ERR_OK) {
            ::dsn::unmarshall(resp, response);
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.decree = response.decree;
            info.server = response.server;
            deleted_count = response.count;
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, deleted_count, std::move(info));
    };
    _client->multi_remove(req,
                          std::move(new_callback),
                          std::chrono::milliseconds(timeout_milliseconds),
                          partition_hash);
}

int pegasus_client_impl::incr(const std::string &hash_key,
                              const std::string &sort_key,
                              int64_t increment,
                              int64_t &new_value,
                              int timeout_milliseconds,
                              int ttl_seconds,
                              internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int _err, int64_t _new_value, internal_info &&_info) {
        ret = _err;
        new_value = _new_value;
        if (info != nullptr)
            (*info) = std::move(_info);
        op_completed.notify();
    };
    async_incr(
        hash_key, sort_key, increment, std::move(callback), timeout_milliseconds, ttl_seconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_incr(const std::string &hash_key,
                                     const std::string &sort_key,
                                     int64_t increment,
                                     async_incr_callback_t &&callback,
                                     int timeout_milliseconds,
                                     int ttl_seconds)
{
    // check params
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, 0, internal_info());
        return;
    }
    if (ttl_seconds < -1) {
        LOG_ERROR("invalid ttl seconds: should be no less than -1, but {}", ttl_seconds);
        if (callback != nullptr)
            callback(PERR_INVALID_ARGUMENT, 0, internal_info());
        return;
    }

    ::dsn::apps::incr_request req;
    pegasus_generate_key(req.key, hash_key, sort_key);
    req.increment = increment;
    if (ttl_seconds <= 0)
        req.expire_ts_seconds = ttl_seconds;
    else
        req.expire_ts_seconds = ttl_seconds + utils::epoch_now();
    auto partition_hash = pegasus_key_hash(req.key);

    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        ::dsn::apps::incr_response response;
        internal_info info;
        if (err == ::dsn::ERR_OK) {
            ::dsn::unmarshall(resp, response);
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.decree = response.decree;
            info.server = response.server;
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, response.new_value, std::move(info));
    };
    _client->incr(req,
                  std::move(new_callback),
                  std::chrono::milliseconds(timeout_milliseconds),
                  partition_hash);
}

int pegasus_client_impl::check_and_set(const std::string &hash_key,
                                       const std::string &check_sort_key,
                                       cas_check_type check_type,
                                       const std::string &check_operand,
                                       const std::string &set_sort_key,
                                       const std::string &set_value,
                                       const check_and_set_options &options,
                                       check_and_set_results &results,
                                       int timeout_milliseconds,
                                       internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int _err, check_and_set_results &&_results, internal_info &&_info) {
        ret = _err;
        results = std::move(_results);
        if (info != nullptr)
            (*info) = std::move(_info);
        op_completed.notify();
    };
    async_check_and_set(hash_key,
                        check_sort_key,
                        check_type,
                        check_operand,
                        set_sort_key,
                        set_value,
                        options,
                        std::move(callback),
                        timeout_milliseconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_check_and_set(const std::string &hash_key,
                                              const std::string &check_sort_key,
                                              cas_check_type check_type,
                                              const std::string &check_operand,
                                              const std::string &set_sort_key,
                                              const std::string &set_value,
                                              const check_and_set_options &options,
                                              async_check_and_set_callback_t &&callback,
                                              int timeout_milliseconds)
{
    // check params
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, check_and_set_results(), internal_info());
        return;
    }

    if (dsn::apps::_cas_check_type_VALUES_TO_NAMES.find(check_type) ==
        dsn::apps::_cas_check_type_VALUES_TO_NAMES.end()) {
        LOG_ERROR("invalid check type: {}", check_type);
        if (callback != nullptr)
            callback(PERR_INVALID_ARGUMENT, check_and_set_results(), internal_info());
        return;
    }

    ::dsn::apps::check_and_set_request req;
    req.hash_key.assign(hash_key.c_str(), 0, hash_key.size());
    req.check_sort_key.assign(check_sort_key.c_str(), 0, check_sort_key.size());
    req.check_type = (dsn::apps::cas_check_type::type)check_type;
    req.check_operand.assign(check_operand.c_str(), 0, check_operand.size());
    if (check_sort_key != set_sort_key) {
        req.set_diff_sort_key = true;
        req.set_sort_key.assign(set_sort_key.c_str(), 0, set_sort_key.size());
    }
    req.set_value.assign(set_value.c_str(), 0, set_value.size());
    if (options.set_value_ttl_seconds == 0)
        req.set_expire_ts_seconds = 0;
    else
        req.set_expire_ts_seconds = options.set_value_ttl_seconds + utils::epoch_now();
    req.return_check_value = options.return_check_value;

    ::dsn::blob tmp_key;
    pegasus_generate_key(tmp_key, req.hash_key, ::dsn::blob());
    auto partition_hash = pegasus_key_hash(tmp_key);
    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        check_and_set_results results;
        internal_info info;
        ::dsn::apps::check_and_set_response response;
        if (err == ::dsn::ERR_OK) {
            ::dsn::unmarshall(resp, response);
            if (response.error == 0) {
                results.set_succeed = true;
            } else if (response.error == 13) { // kTryAgain
                results.set_succeed = false;
                response.error = 0;
            } else {
                results.set_succeed = false;
            }
            if (response.check_value_returned) {
                results.check_value_returned = true;
                if (response.check_value_exist) {
                    results.check_value_exist = true;
                    results.check_value.assign(response.check_value.data(),
                                               response.check_value.length());
                }
            }
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.decree = response.decree;
            info.server = response.server;
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(results), std::move(info));
    };
    _client->check_and_set(req,
                           std::move(new_callback),
                           std::chrono::milliseconds(timeout_milliseconds),
                           partition_hash);
}

int pegasus_client_impl::check_and_mutate(const std::string &hash_key,
                                          const std::string &check_sort_key,
                                          cas_check_type check_type,
                                          const std::string &check_operand,
                                          const mutations &mutations,
                                          const check_and_mutate_options &options,
                                          check_and_mutate_results &results,
                                          int timeout_milliseconds,
                                          internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int _err, check_and_mutate_results &&_results, internal_info &&_info) {
        ret = _err;
        results = std::move(_results);
        if (info != nullptr)
            (*info) = std::move(_info);
        op_completed.notify();
    };
    async_check_and_mutate(hash_key,
                           check_sort_key,
                           check_type,
                           check_operand,
                           mutations,
                           options,
                           std::move(callback),
                           timeout_milliseconds);
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_check_and_mutate(const std::string &hash_key,
                                                 const std::string &check_sort_key,
                                                 cas_check_type check_type,
                                                 const std::string &check_operand,
                                                 const mutations &mutations,
                                                 const check_and_mutate_options &options,
                                                 async_check_and_mutate_callback_t &&callback,
                                                 int timeout_milliseconds)
{
    // check params
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        if (callback != nullptr)
            callback(PERR_INVALID_HASH_KEY, check_and_mutate_results(), internal_info());
        return;
    }

    if (dsn::apps::_cas_check_type_VALUES_TO_NAMES.find(check_type) ==
        dsn::apps::_cas_check_type_VALUES_TO_NAMES.end()) {
        LOG_ERROR("invalid check type: {}", check_type);
        if (callback != nullptr)
            callback(PERR_INVALID_ARGUMENT, check_and_mutate_results(), internal_info());
        return;
    }
    if (mutations.is_empty()) {
        LOG_ERROR("invalid mutations: mutations should not be empty.");
        if (callback != nullptr)
            callback(PERR_INVALID_ARGUMENT, check_and_mutate_results(), internal_info());
        return;
    }

    ::dsn::apps::check_and_mutate_request req;
    req.hash_key.assign(hash_key.c_str(), 0, hash_key.size());
    req.check_sort_key.assign(check_sort_key.c_str(), 0, check_sort_key.size());
    req.check_type = (dsn::apps::cas_check_type::type)check_type;
    req.check_operand.assign(check_operand.c_str(), 0, check_operand.size());

    std::vector<mutate> mutate_list;
    mutations.get_mutations(mutate_list);
    req.mutate_list.resize(mutate_list.size());
    for (int i = 0; i < mutate_list.size(); ++i) {
        auto &mu = mutate_list[i];
        req.mutate_list[i].operation = (dsn::apps::mutate_operation::type)mu.operation;
        req.mutate_list[i].sort_key = blob::create_from_bytes(std::move(mu.sort_key));

        if (mu.operation == mutate::mutate_operation::MO_PUT) {
            req.mutate_list[i].value = blob::create_from_bytes(std::move(mu.value));
            req.mutate_list[i].set_expire_ts_seconds = mu.set_expire_ts_seconds;
        }
    }

    req.return_check_value = options.return_check_value;

    ::dsn::blob tmp_key;
    pegasus_generate_key(tmp_key, req.hash_key, ::dsn::blob());
    auto partition_hash = pegasus_key_hash(tmp_key);
    auto new_callback = [user_callback = std::move(callback)](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        if (user_callback == nullptr) {
            return;
        }
        check_and_mutate_results results;
        internal_info info;
        ::dsn::apps::check_and_mutate_response response;
        if (err == ::dsn::ERR_OK) {
            ::dsn::unmarshall(resp, response);
            if (response.error == 0) {
                results.mutate_succeed = true;
            } else if (response.error == 13) { // kTryAgain
                results.mutate_succeed = false;
                response.error = 0;
            } else {
                results.mutate_succeed = false;
            }
            if (response.check_value_returned) {
                results.check_value_returned = true;
                if (response.check_value_exist) {
                    results.check_value_exist = true;
                    results.check_value.assign(response.check_value.data(),
                                               response.check_value.length());
                }
            }
            info.app_id = response.app_id;
            info.partition_index = response.partition_index;
            info.decree = response.decree;
            info.server = response.server;
        }
        int ret =
            get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
        user_callback(ret, std::move(results), std::move(info));
    };
    _client->check_and_mutate(req,
                              std::move(new_callback),
                              std::chrono::milliseconds(timeout_milliseconds),
                              partition_hash);
}

int pegasus_client_impl::ttl(const std::string &hash_key,
                             const std::string &sort_key,
                             int &ttl_seconds,
                             int timeout_milliseconds,
                             internal_info *info)
{
    // check params
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        return PERR_INVALID_HASH_KEY;
    }

    ::dsn::blob req;
    pegasus_generate_key(req, hash_key, sort_key);
    auto partition_hash = pegasus_key_hash(req);
    auto pr =
        _client->ttl_sync(req, std::chrono::milliseconds(timeout_milliseconds), partition_hash);
    if (pr.first == ERR_OK && pr.second.error == 0) {
        ttl_seconds = pr.second.ttl_seconds;
    }
    if (info != nullptr) {
        if (pr.first == ERR_OK) {
            info->app_id = pr.second.app_id;
            info->partition_index = pr.second.partition_index;
            info->decree = -1;
            info->server = pr.second.server;
        } else {
            info->app_id = -1;
            info->partition_index = -1;
            info->decree = -1;
        }
    }
    return get_client_error(pr.first == ERR_OK ? get_rocksdb_server_error(pr.second.error)
                                               : int(pr.first));
}

void pegasus_client_impl::async_get_scanner(const std::string &hash_key,
                                            const std::string &start_sortkey,
                                            const std::string &stop_sortkey,
                                            const scan_options &options,
                                            async_get_scanner_callback_t &&callback)
{
    if (callback) {
        pegasus_scanner *scanner;
        int ret = get_scanner(hash_key, start_sortkey, stop_sortkey, options, scanner);
        callback(ret, scanner);
    }
}

int pegasus_client_impl::get_scanner(const std::string &hash_key,
                                     const std::string &start_sort_key,
                                     const std::string &stop_sort_key,
                                     const scan_options &options,
                                     pegasus_scanner *&scanner)
{
    // check params
    if (hash_key.size() >= UINT16_MAX) {
        LOG_ERROR("invalid hash key: hash key length should be less than UINT16_MAX, but {}",
                  hash_key.size());
        return PERR_INVALID_HASH_KEY;
    }
    if (hash_key.empty()) {
        LOG_ERROR("invalid hash key: hash key cannot be empty when scan");
        return PERR_INVALID_HASH_KEY;
    }

    ::dsn::blob start;
    ::dsn::blob stop;
    scan_options o(options);

    // generate key range by start_sort_key and stop_sort_key
    pegasus_generate_key(start, hash_key, start_sort_key);
    if (stop_sort_key.empty()) {
        pegasus_generate_next_blob(stop, hash_key);
        o.stop_inclusive = false;
    } else {
        pegasus_generate_key(stop, hash_key, stop_sort_key);
    }

    // limit key range by prefix filter
    if (o.sort_key_filter_type == filter_type::FT_MATCH_PREFIX &&
        o.sort_key_filter_pattern.length() > 0) {
        ::dsn::blob prefix_start, prefix_stop;
        pegasus_generate_key(prefix_start, hash_key, o.sort_key_filter_pattern);
        pegasus_generate_next_blob(prefix_stop, hash_key, o.sort_key_filter_pattern);

        if (::dsn::string_view(prefix_start).compare(start) > 0) {
            start = std::move(prefix_start);
            o.start_inclusive = true;
        }

        if (::dsn::string_view(prefix_stop).compare(stop) <= 0) {
            stop = std::move(prefix_stop);
            o.stop_inclusive = false;
        }
    }

    // check if range is empty
    std::vector<uint64_t> v;
    int c = ::dsn::string_view(start).compare(stop);
    if (c < 0 || (c == 0 && o.start_inclusive && o.stop_inclusive)) {
        v.push_back(pegasus_key_hash(start));
    }
    scanner = new pegasus_scanner_impl(_client, std::move(v), o, start, stop, false, false);

    return PERR_OK;
}

DEFINE_TASK_CODE_RPC(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
                     TASK_PRIORITY_COMMON,
                     ::dsn::THREAD_POOL_DEFAULT)
void pegasus_client_impl::async_get_unordered_scanners(
    int max_split_count,
    const scan_options &options,
    async_get_unordered_scanners_callback_t &&callback)
{
    if (!callback) {
        return;
    }

    // check params
    if (max_split_count <= 0) {
        LOG_ERROR("invalid max_split_count: which should be greater than 0, but {}",
                  max_split_count);
        callback(PERR_INVALID_SPLIT_COUNT, std::vector<pegasus_scanner *>());
        return;
    }

    auto new_callback = [ user_callback = std::move(callback), max_split_count, options, this ](
        ::dsn::error_code err, dsn::message_ex * req, dsn::message_ex * resp)
    {
        std::vector<pegasus_scanner *> scanners;
        query_cfg_response response;
        if (err == ERR_OK) {
            ::dsn::unmarshall(resp, response);
            if (response.err == ERR_OK) {
                unsigned int count = response.partition_count;
                int split = count < max_split_count ? count : max_split_count;
                scanners.resize(split);

                int size = count / split;
                int more = count - size * split;

                for (int i = 0; i < split; i++) {
                    int s = size + (i < more);
                    std::vector<uint64_t> hash(s);
                    for (int j = 0; j < s; j++)
                        hash[j] = --count;
                    scanners[i] =
                        new pegasus_scanner_impl(_client, std::move(hash), options, true, true);
                }
            }
        }
        int ret = get_client_error(err == ERR_OK ? int(response.err) : int(err));
        user_callback(ret, std::move(scanners));
    };

    query_cfg_request req;
    req.app_name = _app_name;
    ::dsn::rpc::call(_meta_server,
                     RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
                     req,
                     nullptr,
                     new_callback,
                     std::chrono::milliseconds(options.timeout_ms),
                     0,
                     0);
}

int pegasus_client_impl::get_unordered_scanners(int max_split_count,
                                                const scan_options &options,
                                                std::vector<pegasus_scanner *> &scanners)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](int err, std::vector<pegasus_scanner *> &&ss) {
        ret = err;
        scanners = std::move(ss);
        op_completed.notify();
    };
    async_get_unordered_scanners(max_split_count, options, std::move(callback));
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::async_duplicate(dsn::apps::duplicate_rpc rpc,
                                          std::function<void(dsn::error_code)> &&callback,
                                          dsn::task_tracker *tracker)
{
    _client->duplicate(rpc, std::move(callback), tracker);
}

const char *pegasus_client_impl::get_error_string(int error_code) const
{
    auto it = _client_error_to_string.find(error_code);
    CHECK(it != _client_error_to_string.end(), "client error {} have no error string", error_code);
    return it->second.c_str();
}

/*static*/ void pegasus_client_impl::init_error()
{
    _client_error_to_string.clear();
#define PEGASUS_ERR_CODE(x, y, z) _client_error_to_string[y] = z
#include <pegasus/error_def.h>
#undef PEGASUS_ERR_CODE

    _server_error_to_client.clear();
    _server_error_to_client[::dsn::ERR_OK] = PERR_OK;
    _server_error_to_client[::dsn::ERR_TIMEOUT] = PERR_TIMEOUT;
    _server_error_to_client[::dsn::ERR_FILE_OPERATION_FAILED] = PERR_SERVER_INTERNAL_ERROR;
    _server_error_to_client[::dsn::ERR_INVALID_STATE] = PERR_SERVER_CHANGED;
    _server_error_to_client[::dsn::ERR_OBJECT_NOT_FOUND] = PERR_OBJECT_NOT_FOUND;
    _server_error_to_client[::dsn::ERR_NETWORK_FAILURE] = PERR_NETWORK_FAILURE;
    _server_error_to_client[::dsn::ERR_HANDLER_NOT_FOUND] = PERR_HANDLER_NOT_FOUND;
    _server_error_to_client[::dsn::ERR_OPERATION_DISABLED] = PERR_OPERATION_DISABLED;
    _server_error_to_client[::dsn::ERR_NOT_ENOUGH_MEMBER] = PERR_NOT_ENOUGH_MEMBER;

    _server_error_to_client[::dsn::ERR_APP_NOT_EXIST] = PERR_APP_NOT_EXIST;
    _server_error_to_client[::dsn::ERR_APP_EXIST] = PERR_APP_EXIST;
    _server_error_to_client[::dsn::ERR_BUSY] = PERR_APP_BUSY;
    _server_error_to_client[::dsn::ERR_SPLITTING] = PERR_APP_SPLITTING;
    _server_error_to_client[::dsn::ERR_DISK_INSUFFICIENT] = PERR_DISK_INSUFFICIENT;

    // rocksdb error;
    for (int i = 1001; i < 1013; i++) {
        _server_error_to_client[-i] = -i;
    }
}

/*static*/ int pegasus_client_impl::get_client_error(int server_error)
{
    auto it = _server_error_to_client.find(server_error);
    if (it != _server_error_to_client.end())
        return it->second;
    LOG_ERROR("can't find corresponding client error definition, server error:[{}:{}]",
              server_error,
              ::dsn::error_code(server_error));
    return PERR_UNKNOWN;
}

/*static*/ int pegasus_client_impl::get_rocksdb_server_error(int rocskdb_error)
{
    return (rocskdb_error == 0) ? 0 : ROCSKDB_ERROR_START - rocskdb_error;
}
} // namespace client
} // namespace pegasus
