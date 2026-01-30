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

/**
 * @file pegasus_client_impl.h
 * @brief Public C++ client interfaces for Pegasus.
 *
 * This file contains the concrete client implementation as well as scanner
 * utilities that are exposed to users. The declarations are annotated with
 * Doxygen-friendly comments so that C++ client documentation can be generated
 * directly via Doxygen.
 *
 * @addtogroup pegasus_cpp_client
 * @{
 */

#pragma once

#include <pegasus/client.h>
#include <rrdb/rrdb.client.h>
#include <stdint.h>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "rpc/rpc_host_port.h"
#include "rrdb/rrdb_types.h"
#include "utils/blob.h"
#include "utils/zlocks.h"

namespace dsn {
class error_code;
class message_ex;
class task_tracker;
} // namespace dsn

namespace pegasus::client {

// Forward declarations
struct internal_info;
struct check_and_set_results;
struct check_and_mutate_results;
class pegasus_scanner;
class abstract_pegasus_scanner;

/**
 * @brief Callback for async set operations.
 * @param err Operation result code (0 for success).
 * @param hashkey Hash key of the request.
 * @param sortkey Sort key of the request.
 * @param value Value passed to set.
 */
using async_set_callback_t =
    std::function<void(int, std::string &&, std::string &&, std::string &&)>;

/**
 * @brief Callback for async get operations.
 * @param err Operation result code (0 for success).
 * @param hashkey Hash key of the request.
 * @param sortkey Sort key of the request.
 * @param value Retrieved value.
 */
using async_get_callback_t =
    std::function<void(int, std::string &&, std::string &&, std::string &&)>;

/**
 * @brief Callback for async multi-get operations.
 * @param err Operation result code (0 for success).
 * @param hashkey Hash key of the request.
 * @param values Retrieved key-value map.
 */
using async_multi_get_callback_t =
    std::function<void(int, std::string &&, std::map<std::string, std::string> &&)>;

/**
 * @brief Callback for async multi-get sortkeys operations.
 * @param err Operation result code (0 for success).
 * @param hashkey Hash key of the request.
 * @param sortkeys Retrieved sort key set.
 */
using async_multi_get_sortkeys_callback_t =
    std::function<void(int, std::string &&, std::set<std::string> &&)>;

/**
 * @brief Callback for async delete operations.
 * @param err Operation result code (0 for success).
 * @param hashkey Hash key of the request.
 * @param sortkey Sort key of the request.
 */
using async_del_callback_t = std::function<void(int, std::string &&, std::string &&)>;

/**
 * @brief Callback for async increment operations.
 * @param err Operation result code (0 for success).
 * @param sortkey Sort key of the request.
 * @param new_value Value after increment.
 */
using async_incr_callback_t = std::function<void(int, std::string &&, int64_t)>;

/**
 * @brief Callback for async check-and-set operations.
 * @param err Operation result code (0 for success).
 * @param hashkey Hash key of the request.
 * @param results Result payload of the operation.
 */
using async_check_and_set_callback_t =
    std::function<void(int, std::string &&, check_and_set_results &&)>;

/**
 * @brief Callback for async check-and-mutate operations.
 * @param err Operation result code (0 for success).
 * @param hashkey Hash key of the request.
 * @param results Result payload of the operation.
 */
using async_check_and_mutate_callback_t =
    std::function<void(int, std::string &&, check_and_mutate_results &&)>;

/**
 * @brief Callback for async scanner creation.
 * @param err Operation result code (0 for success).
 * @param scanner Newly created scanner instance.
 */
using async_get_scanner_callback_t = std::function<void(int, pegasus_scanner *)>;

/**
 * @brief Callback for async unordered scanner creation.
 * @param err Operation result code (0 for success).
 * @param scanners List of scanners for parallel scan.
 */
using async_get_unordered_scanners_callback_t =
    std::function<void(int, std::vector<pegasus_scanner *> &&)>;

/**
 * @brief Callback for async scanner next calls.
 * @param err Operation result code (0 for success).
 * @param hashkey Hash key of the scanned record.
 * @param sortkey Sort key of the scanned record.
 * @param value Value of the scanned record.
 */
using async_scan_next_callback_t =
    std::function<void(int, std::string &&, std::string &&, std::string &&)>;

/**
 * @brief The implementation class of Pegasus client.
 *
 * This class provides the concrete implementation of all Pegasus client operations,
 * including data access APIs like get/set/delete and scan operations. It communicates
 * with the Pegasus server cluster to perform these operations.
 *
 * @ingroup pegasus_cpp_client
 *
 * Example usage:
 * @code
 * auto *client = pegasus::client::pegasus_client_factory::create_client(
 *     "cluster", "table", "config.ini");
 * std::string value;
 * int rc = client->get("hash", "sort", value);
 * @endcode
 */
class pegasus_client_impl : public pegasus_client
{
public:
    /**
     * @brief Construct a new pegasus client impl object
     *
     * @param cluster_name Name of the Pegasus cluster to connect to
     * @param app_name Name of the Pegasus table (app) to operate on
     */
    pegasus_client_impl(const char *cluster_name, const char *app_name);
    ~pegasus_client_impl() override;

    /**
     * @brief Get the cluster name this client is connected to
     * @return const char* The cluster name
     */
    const char *get_cluster_name() const override;

    /**
     * @brief Get the table (app) name this client is operating on
     * @return const char* The table name
     */
    const char *get_app_name() const override;

    /**
     * @brief Set a key-value pair in Pegasus
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param value The value to set
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param ttl_seconds Time-to-live in seconds, 0 means no TTL (default 0)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int set(const std::string &hashkey,
            const std::string &sortkey,
            const std::string &value,
            int timeout_milliseconds = 5000,
            int ttl_seconds = 0,
            internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously set a key-value pair in Pegasus
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param value The value to set
     * @param callback Callback function to handle the async result
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param ttl_seconds Time-to-live in seconds, 0 means no TTL (default 0)
     */
    void async_set(const std::string &hashkey,
                   const std::string &sortkey,
                   const std::string &value,
                   async_set_callback_t &&callback = nullptr,
                   int timeout_milliseconds = 5000,
                   int ttl_seconds = 0) override;

    /**
     * @brief Set multiple key-value pairs in batch
     *
     * @param hashkey The hash key for all key-value pairs
     * @param kvs Map of sortkey-value pairs to set
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param ttl_seconds Time-to-live in seconds for all values, 0 means no TTL (default 0)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int multi_set(const std::string &hashkey,
                  const std::map<std::string, std::string> &kvs,
                  int timeout_milliseconds = 5000,
                  int ttl_seconds = 0,
                  internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously set multiple key-value pairs in batch
     *
     * @param hashkey The hash key for all key-value pairs
     * @param kvs Map of sortkey-value pairs to set
     * @param callback Callback function to handle the async result
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param ttl_seconds Time-to-live in seconds for all values, 0 means no TTL (default 0)
     */
    void async_multi_set(const std::string &hashkey,
                         const std::map<std::string, std::string> &kvs,
                         async_multi_set_callback_t &&callback = nullptr,
                         int timeout_milliseconds = 5000,
                         int ttl_seconds = 0) override;

    /**
     * @brief Get a value from Pegasus
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param value Output parameter for the retrieved value
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int get(const std::string &hashkey,
            const std::string &sortkey,
            std::string &value,
            int timeout_milliseconds = 5000,
            internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously get a value from Pegasus
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param callback Callback function to handle the async result
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     */
    void async_get(const std::string &hashkey,
                   const std::string &sortkey,
                   async_get_callback_t &&callback = nullptr,
                   int timeout_milliseconds = 5000) override;

    /**
     * @brief Get multiple values by sort keys
     *
     * @param hashkey The hash key
     * @param sortkeys Set of sort keys to retrieve
     * @param values Output map for retrieved key-value pairs
     * @param max_fetch_count Maximum number of items to fetch (default 100)
     * @param max_fetch_size Maximum total size of fetched data in bytes (default 1000000)
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int multi_get(const std::string &hashkey,
                  const std::set<std::string> &sortkeys,
                  std::map<std::string, std::string> &values,
                  int max_fetch_count = 100,
                  int max_fetch_size = 1000000,
                  int timeout_milliseconds = 5000,
                  internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously get multiple values by sort keys
     *
     * @param hashkey The hash key
     * @param sortkeys Set of sort keys to retrieve
     * @param callback Callback function to handle the async result
     * @param max_fetch_count Maximum number of items to fetch (default 100)
     * @param max_fetch_size Maximum total size of fetched data in bytes (default 1000000)
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     */
    void async_multi_get(const std::string &hashkey,
                         const std::set<std::string> &sortkeys,
                         async_multi_get_callback_t &&callback = nullptr,
                         int max_fetch_count = 100,
                         int max_fetch_size = 1000000,
                         int timeout_milliseconds = 5000) override;

    /**
     * @brief Get multiple values by sort key range
     *
     * @param hashkey The hash key
     * @param start_sortkey Start sort key of the range (inclusive)
     * @param stop_sortkey Stop sort key of the range (exclusive)
     * @param options Multi-get options like sort direction
     * @param values Output map for retrieved key-value pairs
     * @param max_fetch_count Maximum number of items to fetch (default 100)
     * @param max_fetch_size Maximum total size of fetched data in bytes (default 1000000)
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int multi_get(const std::string &hashkey,
                  const std::string &start_sortkey,
                  const std::string &stop_sortkey,
                  const multi_get_options &options,
                  std::map<std::string, std::string> &values,
                  int max_fetch_count = 100,
                  int max_fetch_size = 1000000,
                  int timeout_milliseconds = 5000,
                  internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously get multiple values by sort key range
     *
     * @param hashkey The hash key
     * @param start_sortkey Start sort key of the range (inclusive)
     * @param stop_sortkey Stop sort key of the range (exclusive)
     * @param options Multi-get options like sort direction
     * @param callback Callback function to handle the async result
     * @param max_fetch_count Maximum number of items to fetch (default 100)
     * @param max_fetch_size Maximum total size of fetched data in bytes (default 1000000)
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     */
    void async_multi_get(const std::string &hashkey,
                         const std::string &start_sortkey,
                         const std::string &stop_sortkey,
                         const multi_get_options &options,
                         async_multi_get_callback_t &&callback = nullptr,
                         int max_fetch_count = 100,
                         int max_fetch_size = 1000000,
                         int timeout_milliseconds = 5000) override;

    /**
     * @brief Get multiple sort keys for a hash key
     *
     * @param hashkey The hash key
     * @param sortkeys Output set for retrieved sort keys
     * @param max_fetch_count Maximum number of items to fetch (default 100)
     * @param max_fetch_size Maximum total size of fetched data in bytes (default 1000000)
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int multi_get_sortkeys(const std::string &hashkey,
                           std::set<std::string> &sortkeys,
                           int max_fetch_count = 100,
                           int max_fetch_size = 1000000,
                           int timeout_milliseconds = 5000,
                           internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously get multiple sort keys
     *
     * @param hashkey The hash key
     * @param callback Callback function to handle the async result
     * @param max_fetch_count Maximum number of items to fetch (default 100)
     * @param max_fetch_size Maximum total size of fetched data in bytes (default 1000000)
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     */
    void async_multi_get_sortkeys(const std::string &hashkey,
                                  async_multi_get_sortkeys_callback_t &&callback = nullptr,
                                  int max_fetch_count = 100,
                                  int max_fetch_size = 1000000,
                                  int timeout_milliseconds = 5000) override;

    /**
     * @brief Check if a key-value pair exists
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for exists, non-zero otherwise)
     */
    int exist(const std::string &hashkey,
              const std::string &sortkey,
              int timeout_milliseconds = 5000,
              internal_info *info = nullptr) override;

    /**
     * @brief Get count of sort keys for a hash key
     *
     * @param hashkey The hash key
     * @param count Output parameter for the count of sort keys
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int sortkey_count(const std::string &hashkey,
                      int64_t &count,
                      int timeout_milliseconds = 5000,
                      internal_info *info = nullptr) override;

    /**
     * @brief Delete a key-value pair
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int del(const std::string &hashkey,
            const std::string &sortkey,
            int timeout_milliseconds = 5000,
            internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously delete a key-value pair
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param callback Callback function to handle the async result
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     */
    void async_del(const std::string &hashkey,
                   const std::string &sortkey,
                   async_del_callback_t &&callback = nullptr,
                   int timeout_milliseconds = 5000) override;

    /**
     * @brief Delete multiple key-value pairs
     *
     * @param hashkey The hash key
     * @param sortkeys Set of sort keys to delete
     * @param deleted_count Output parameter for count of successfully deleted items
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int multi_del(const std::string &hashkey,
                  const std::set<std::string> &sortkeys,
                  int64_t &deleted_count,
                  int timeout_milliseconds = 5000,
                  internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously delete multiple key-value pairs
     *
     * @param hashkey The hash key
     * @param sortkeys Set of sort keys to delete
     * @param callback Callback function to handle the async result
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     */
    void async_multi_del(const std::string &hashkey,
                         const std::set<std::string> &sortkeys,
                         async_multi_del_callback_t &&callback = nullptr,
                         int timeout_milliseconds = 5000) override;

    /**
     * @brief Increment a counter value
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param increment The value to increment by
     * @param new_value Output parameter for the new counter value
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param ttl_seconds Time-to-live in seconds, 0 means no TTL (default 0)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int incr(const std::string &hashkey,
             const std::string &sortkey,
             int64_t increment,
             int64_t &new_value,
             int timeout_milliseconds = 5000,
             int ttl_seconds = 0,
             internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously increment a counter value
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param increment The value to increment by
     * @param callback Callback function to handle the async result
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param ttl_seconds Time-to-live in seconds, 0 means no TTL (default 0)
     */
    void async_incr(const std::string &hashkey,
                    const std::string &sortkey,
                    int64_t increment,
                    async_incr_callback_t &&callback = nullptr,
                    int timeout_milliseconds = 5000,
                    int ttl_seconds = 0) override;

    /**
     * @brief Atomically check and set a value (CAS operation)
     *
     * @param hash_key The hash key
     * @param check_sort_key The sort key to check
     * @param check_type Type of check to perform (e.g. EQ, LE, GE)
     * @param check_operand Value to compare against
     * @param set_sort_key The sort key to set if check passes
     * @param set_value The value to set if check passes
     * @param options Additional options for the operation
     * @param results Output parameter for operation results
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int check_and_set(const std::string &hash_key,
                      const std::string &check_sort_key,
                      cas_check_type check_type,
                      const std::string &check_operand,
                      const std::string &set_sort_key,
                      const std::string &set_value,
                      const check_and_set_options &options,
                      check_and_set_results &results,
                      int timeout_milliseconds = 5000,
                      internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously atomically check and set a value (CAS operation)
     *
     * @param hash_key The hash key
     * @param check_sort_key The sort key to check
     * @param check_type Type of check to perform (e.g. EQ, LE, GE)
     * @param check_operand Value to compare against
     * @param set_sort_key The sort key to set if check passes
     * @param set_value The value to set if check passes
     * @param options Additional options for the operation
     * @param callback Callback function to handle the async result
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     */
    void async_check_and_set(const std::string &hash_key,
                             const std::string &check_sort_key,
                             cas_check_type check_type,
                             const std::string &check_operand,
                             const std::string &set_sort_key,
                             const std::string &set_value,
                             const check_and_set_options &options,
                             async_check_and_set_callback_t &&callback = nullptr,
                             int timeout_milliseconds = 5000) override;

    /**
     * @brief Atomically check and perform multiple mutations
     *
     * @param hash_key The hash key
     * @param check_sort_key The sort key to check
     * @param check_type Type of check to perform (e.g. EQ, LE, GE)
     * @param check_operand Value to compare against
     * @param mutations List of mutations to perform if check passes
     * @param options Additional options for the operation
     * @param results Output parameter for operation results
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int check_and_mutate(const std::string &hash_key,
                         const std::string &check_sort_key,
                         cas_check_type check_type,
                         const std::string &check_operand,
                         const mutations &mutations,
                         const check_and_mutate_options &options,
                         check_and_mutate_results &results,
                         int timeout_milliseconds = 5000,
                         internal_info *info = nullptr) override;

    /**
     * @brief Asynchronously atomically check and perform multiple mutations
     *
     * @param hash_key The hash key
     * @param check_sort_key The sort key to check
     * @param check_type Type of check to perform (e.g. EQ, LE, GE)
     * @param check_operand Value to compare against
     * @param mutations List of mutations to perform if check passes
     * @param options Additional options for the operation
     * @param callback Callback function to handle the async result
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     */
    void async_check_and_mutate(const std::string &hash_key,
                                const std::string &check_sort_key,
                                cas_check_type check_type,
                                const std::string &check_operand,
                                const mutations &mutations,
                                const check_and_mutate_options &options,
                                async_check_and_mutate_callback_t &&callback = nullptr,
                                int timeout_milliseconds = 5000) override;

    /**
     * @brief Get time-to-live (TTL) for a key-value pair
     *
     * @param hashkey The hash key
     * @param sortkey The sort key
     * @param ttl_seconds Output parameter for TTL in seconds (-1 for no TTL)
     * @param timeout_milliseconds Operation timeout in milliseconds (default 5000)
     * @param info Optional internal info output
     * @return int Operation result code (0 for success)
     */
    int ttl(const std::string &hashkey,
            const std::string &sortkey,
            int &ttl_seconds,
            int timeout_milliseconds = 5000,
            internal_info *info = nullptr) override;

    /**
     * @brief Get a scanner for range query
     *
     * @param hashkey The hash key to scan
     * @param start_sortkey Start sort key of the range (inclusive)
     * @param stop_sortkey Stop sort key of the range (exclusive)
     * @param options Scan options like sort direction
     * @param scanner Output parameter for the created scanner
     * @return int Operation result code (0 for success)
     */
    int get_scanner(const std::string &hashkey,
                    const std::string &start_sortkey,
                    const std::string &stop_sortkey,
                    const scan_options &options,
                    pegasus_scanner *&scanner) override;

    /**
     * @brief Asynchronously get a scanner for range query
     *
     * @param hashkey The hash key to scan
     * @param start_sortkey Start sort key of the range (inclusive)
     * @param stop_sortkey Stop sort key of the range (exclusive)
     * @param options Scan options like sort direction
     * @param callback Callback function to handle the async result
     */
    void async_get_scanner(const std::string &hashkey,
                           const std::string &start_sortkey,
                           const std::string &stop_sortkey,
                           const scan_options &options,
                           async_get_scanner_callback_t &&callback) override;

    /**
     * @brief Get multiple scanners for parallel scanning across partitions
     *
     * @param max_split_count Maximum number of scanners to create (partitions to scan)
     * @param options Scan options like sort direction
     * @param scanners Output vector for the created scanners
     * @return int Operation result code (0 for success)
     */
    int get_unordered_scanners(int max_split_count,
                               const scan_options &options,
                               std::vector<pegasus_scanner *> &scanners) override;

    /**
     * @brief Asynchronously get multiple scanners for parallel scanning
     *
     * @param max_split_count Maximum number of scanners to create (partitions to scan)
     * @param options Scan options like sort direction
     * @param callback Callback function to handle the async result
     */
    void async_get_unordered_scanners(
        int max_split_count,
        const scan_options &options,
        async_get_unordered_scanners_callback_t &&callback) override;

    /**
     * @internal
     * @brief Internal method for data duplication
     * @param rpc Duplication RPC request
     * @param callback Callback function for async result
     * @param tracker Task tracker for managing async operations
     * @see pegasus::server::pegasus_mutation_duplicator
     */
    void async_duplicate(dsn::apps::duplicate_rpc rpc,
                         std::function<void(dsn::error_code)> &&callback,
                         dsn::task_tracker *tracker);

    /**
     * @brief Get error description string for error code
     *
     * @param error_code The error code to lookup
     * @return const char* Description of the error
     */
    [[nodiscard]] const char *get_error_string(int error_code) const override;

    /**
     * @brief Initialize error code mappings
     *
     * This method initializes the mapping between server error codes
     * and client error codes, and should be called once during startup.
     */
    static void init_error();

    /**
     * @brief Implementation of Pegasus scanner for range queries
     *
     * This class provides the concrete implementation for scanning key ranges in Pegasus.
     * It supports both synchronous and asynchronous scanning operations, with options
     * for full scans or partial range scans.
     */
    class pegasus_scanner_impl : public pegasus_scanner
    {
    public:
        /**
         * @brief Get next key-value pair from scanner
         * @param hashkey Output parameter for hash key
         * @param sortkey Output parameter for sort key
         * @param value Output parameter for value
         * @param info Optional internal info output
         * @return int Operation result code (0 for success)
         */
        int next(std::string &hashkey,
                 std::string &sortkey,
                 std::string &value,
                 internal_info *info) override;

        /**
         * @brief Get count of remaining items in current batch
         *
         * @param count Output parameter for item count
         * @param info Optional internal info output
         * @return int Operation result code (0 for success)
         */
        int next(int32_t &count, internal_info *info) override;

        /**
         * @brief Asynchronously get next key-value pair from scanner.
         * @param callback Callback function to handle the async result
         * The callback parameters are:
         *   - error_code: Operation result code (0 for success)
         *   - hashkey: The hash key of the scanned item
         *   - sortkey: The sort key of the scanned item
         *   - value: The value of the scanned item
         */
        void async_next(async_scan_next_callback_t &&callback) override;

        /**
         * @brief Check if the scanner can be safely destroyed
         * @return bool True if scanner can be safely destroyed, false otherwise
         */
        bool safe_destructible() const override;

        /**
         * @brief Get a smart pointer wrapper for the scanner
         * @return pegasus_scanner_wrapper A wrapper object that manages the scanner's lifetime
         */
        pegasus_scanner_wrapper get_smart_wrapper() override;

        ~pegasus_scanner_impl() override;

        /**
         * @brief Construct a scanner for full range scan
         * @param client The RPC client for communicating with Pegasus servers
         * @param hash Vector of partition hashes to scan
         * @param options Scan configuration options
         * @param validate_partition_hash Whether to validate partition hash
         * @param full_scan Whether this is a full scan operation
         */
        pegasus_scanner_impl(::dsn::apps::rrdb_client *client,
                             std::vector<uint64_t> &&hash,
                             const scan_options &options,
                             bool validate_partition_hash,
                             bool full_scan);

        /**
         * @brief Construct a scanner for range scan
         * @param client The RPC client for communicating with Pegasus servers
         * @param hash Vector of partition hashes to scan
         * @param options Scan configuration options
         * @param start_key The start key of scan range (inclusive)
         * @param stop_key The stop key of scan range (exclusive)
         * @param validate_partition_hash Whether to validate partition hash
         * @param full_scan Whether this is a full scan operation
         */
        pegasus_scanner_impl(::dsn::apps::rrdb_client *client,
                             std::vector<uint64_t> &&hash,
                             const scan_options &options,
                             const ::dsn::blob &start_key,
                             const ::dsn::blob &stop_key,
                             bool validate_partition_hash,
                             bool full_scan);

        pegasus_scanner_impl(const pegasus_scanner_impl &) = delete;
        pegasus_scanner_impl &operator=(const pegasus_scanner_impl &) = delete;
        pegasus_scanner_impl(pegasus_scanner_impl &&) = delete;
        pegasus_scanner_impl &operator=(pegasus_scanner_impl &&) = delete;

    private:
        enum class async_scan_type : char
        {
            NORMAL,
            COUNT_ONLY,
            COUNT_ONLY_FINISHED
        };

        ::dsn::apps::rrdb_client *_client;
        ::dsn::blob _start_key;
        ::dsn::blob _stop_key;
        scan_options _options;
        std::vector<uint64_t> _splits_hash;

        uint64_t _hash;
        std::vector<::dsn::apps::key_value> _kvs;
        internal_info _info;
        int32_t _p;
        int32_t _kv_count;

        int64_t _context;
        mutable ::dsn::zlock _lock;
        std::list<async_scan_next_callback_t> _queue;
        volatile bool _rpc_started;
        bool _validate_partition_hash;
        bool _full_scan;
        async_scan_type _type;

        void _async_next_internal();
        void _start_scan();
        void _next_batch();
        void _on_scan_response(::dsn::error_code, dsn::message_ex *, dsn::message_ex *);
        void _split_reset();
        static const char _holder[];
        static const ::dsn::blob _min;
        static const ::dsn::blob _max;
    };

    /**
     * @brief Convert server error code to client error code
     *
     * @param server_error Server-side error code
     * @return int Corresponding client error code
     */
    static int get_client_error(int server_error);

    /**
     * @brief Convert RocksDB error code to Pegasus server error code
     *
     * @param rocskdb_error RocksDB error code
     * @return int Corresponding Pegasus server error code
     */
    static int get_rocksdb_server_error(int rocskdb_error);

private:
    /**
     * @internal
     * @brief Wrapper class for pegasus_scanner to provide smart pointer semantics
     */
    class pegasus_scanner_impl_wrapper : public abstract_pegasus_scanner
    {
        std::shared_ptr<pegasus_scanner> _p;

    public:
        pegasus_scanner_impl_wrapper(pegasus_scanner *p) : _p(p) {}

        void async_next(async_scan_next_callback_t &&callback) override;

        int next(int32_t &count, internal_info *info) override
        {
            return _p->next(count, info);
        }

        int next(std::string &hashkey,
                 std::string &sortkey,
                 std::string &value,
                 internal_info *info) override
        {
            return _p->next(hashkey, sortkey, value, info);
        }
    };

    std::string _cluster_name;
    std::string _app_name;
    ::dsn::host_port _meta_server;
    ::dsn::apps::rrdb_client *_client;

    /**
     * @brief Mapping from client error codes to their string descriptions
     *
     * This map stores the string representations of all client error codes,
     * used by get_error_string() to provide human-readable error messages.
     */
    static std::unordered_map<int, std::string> _client_error_to_string;

    /**
     * @brief Mapping from server error codes to client error codes
     *
     * This map provides translation between server-side error codes and client-side error codes.
     * It is initialized once during client library initialization via init_error().
     */
    static std::unordered_map<int, int> _server_error_to_client;
};
} // namespace pegasus::client

/** @} */
