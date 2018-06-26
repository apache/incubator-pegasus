// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <vector>
#include <set>
#include <map>
#include <stdint.h>
#include <pegasus/error.h>
#include <functional>
#include <memory>

namespace pegasus {

class rrdb_client;
///
/// \brief The client class
/// pegasus_client is the base class that users use to access a specific cluster with an app name
/// the class of client provides the basic operation to:
/// set/get/delete the value of a key in a app.
///
class pegasus_client
{
public:
    struct internal_info
    {
        int32_t app_id;
        int32_t partition_index;
        int64_t decree;
        std::string server;
        internal_info() : app_id(-1), partition_index(-1), decree(-1) {}
        internal_info(internal_info &&_info)
        {
            app_id = _info.app_id;
            partition_index = _info.partition_index;
            decree = _info.decree;
            server = std::move(_info.server);
        }
        internal_info(const internal_info &_info)
        {
            app_id = _info.app_id;
            partition_index = _info.partition_index;
            decree = _info.decree;
            server = _info.server;
        }
        const internal_info &operator=(const internal_info &other)
        {
            app_id = other.app_id;
            partition_index = other.partition_index;
            decree = other.decree;
            server = other.server;
            return *this;
        }
        const internal_info &operator=(internal_info &&_info)
        {
            app_id = _info.app_id;
            partition_index = _info.partition_index;
            decree = _info.decree;
            server = std::move(_info.server);
            return *this;
        }
    };

    enum filter_type
    {
        FT_NO_FILTER = 0,
        FT_MATCH_ANYWHERE = 1,
        FT_MATCH_PREFIX = 2,
        FT_MATCH_POSTFIX = 3
    };

    struct multi_get_options
    {
        bool start_inclusive;
        bool stop_inclusive;
        filter_type sort_key_filter_type;
        std::string sort_key_filter_pattern;
        bool no_value; // only fetch hash_key and sort_key, but not fetch value
        bool reverse;  // if search in reverse direction
        multi_get_options()
            : start_inclusive(true),
              stop_inclusive(false),
              sort_key_filter_type(FT_NO_FILTER),
              no_value(false),
              reverse(false)
        {
        }
        multi_get_options(const multi_get_options &o)
            : start_inclusive(o.start_inclusive),
              stop_inclusive(o.stop_inclusive),
              sort_key_filter_type(o.sort_key_filter_type),
              sort_key_filter_pattern(o.sort_key_filter_pattern),
              no_value(o.no_value),
              reverse(o.reverse)
        {
        }
    };

    struct scan_options
    {
        int timeout_ms;           // RPC call timeout param, in milliseconds
        int batch_size;           // max k-v count one RPC call
        bool start_inclusive;     // will be ingored when get_unordered_scanners()
        bool stop_inclusive;      // will be ingored when get_unordered_scanners()
        filter_type hash_key_filter_type;
        std::string hash_key_filter_pattern;
        filter_type sort_key_filter_type;
        std::string sort_key_filter_pattern;
        bool no_value; // only fetch hash_key and sort_key, but not fetch value
        scan_options()
            : timeout_ms(5000),
              batch_size(100),
              start_inclusive(true),
              stop_inclusive(false),
              hash_key_filter_type(FT_NO_FILTER),
              sort_key_filter_type(FT_NO_FILTER),
              no_value(false)
        {
        }
        scan_options(const scan_options &o)
            : timeout_ms(o.timeout_ms),
              batch_size(o.batch_size),
              start_inclusive(o.start_inclusive),
              stop_inclusive(o.stop_inclusive),
              hash_key_filter_type(o.hash_key_filter_type),
              hash_key_filter_pattern(o.hash_key_filter_pattern),
              sort_key_filter_type(o.sort_key_filter_type),
              sort_key_filter_pattern(o.sort_key_filter_pattern),
              no_value(o.no_value)
        {
        }
    };

    class pegasus_scanner;

    // define callback function types for asynchronous operations.
    typedef std::function<void(int /*error_code*/, internal_info && /*info*/)> async_set_callback_t;
    typedef std::function<void(int /*error_code*/, internal_info && /*info*/)>
        async_multi_set_callback_t;
    typedef std::function<void(
        int /*error_code*/, std::string && /*value*/, internal_info && /*info*/)>
        async_get_callback_t;
    typedef std::function<void(int /*error_code*/,
                               std::map<std::string, std::string> && /*values*/,
                               internal_info && /*info*/)>
        async_multi_get_callback_t;
    typedef std::function<void(
        int /*error_code*/, std::set<std::string> && /*sortkeys*/, internal_info && /*info*/)>
        async_multi_get_sortkeys_callback_t;
    typedef std::function<void(int /*error_code*/, internal_info && /*info*/)> async_del_callback_t;
    typedef std::function<void(
        int /*error_code*/, int64_t /*deleted_count*/, internal_info && /*info*/)>
        async_multi_del_callback_t;
    typedef std::function<void(int /*error_code*/,
                               std::string && /*hash_key*/,
                               std::string && /*sort_key*/,
                               std::string && /*value*/,
                               internal_info && /*info*/)>
        async_scan_next_callback_t;
    typedef std::function<void(int /*error_code*/, pegasus_scanner * /*hash_scanner*/)>
        async_get_scanner_callback_t;
    typedef std::function<void(int /*error_code*/, std::vector<pegasus_scanner *> && /*scanners*/)>
        async_get_unordered_scanners_callback_t;

    class abstract_pegasus_scanner
    {
    public:
        ///
        /// \brief get the next key-value pair of this scanner
        /// thread-safe
        /// \param hashkey
        /// used to decide which partition to put this k-v
        /// \param sortkey
        /// all the k-v under hashkey will be sorted by sortkey.
        /// \param value
        /// corresponding value
        /// \return
        /// int, the error indicates whether or not the operation is succeeded.
        /// this error can be converted to a string using get_error_string()
        /// PEER_OK means a valid k-v pair got
        /// PERR_SCAN_COMPLETE means all k-v have been iterated before this call
        /// otherwise some error orrured
        ///
        virtual int next(std::string &hashkey,
                         std::string &sortkey,
                         std::string &value,
                         internal_info *info = NULL) = 0;

        ///
        /// \brief async get the next key-value pair of this scanner
        /// thread-safe
        /// \param callback
        /// status and result will be passed to callback
        /// status(PEER_OK) means a valid k-v pair got
        /// status(PERR_SCAN_COMPLETE) means all k-v have been iterated before this call
        /// otherwise some error orrured
        ///
        virtual void async_next(async_scan_next_callback_t &&callback) = 0;

        virtual ~abstract_pegasus_scanner() {}
    };

    typedef std::shared_ptr<abstract_pegasus_scanner> pegasus_scanner_wrapper;

    class pegasus_scanner : public abstract_pegasus_scanner
    {
    public:
        ///
        /// \brief scanner could be deleted safely when this method returned true
        /// until another async_next() or next() called
        /// otherwise, this scanner object may be used by some callback of async_next() later,
        /// in that case, this object CANNOT be destructed even if it won't be referenced anymore
        ///
        virtual bool safe_destructible() const = 0;

        ///
        /// \brief get a smart wrapper of scanner which could be used like shared_ptr
        /// then users do not need bother of the scanner object destruction
        /// this method should be called after scanner pointer generated immediately,
        /// and the original scanner pointer should not be used anymore
        ///
        virtual pegasus_scanner_wrapper get_smart_wrapper() = 0;
    };

public:
    // destructor
    virtual ~pegasus_client() {}

    ///
    /// \brief get_app_name
    /// \return cluster_name
    ///
    virtual const char *get_cluster_name() const = 0;

    ///
    /// \brief get_app_name
    /// an app is a logical isolated table.
    /// a cluster can have multiple apps.
    /// \return app_name
    ///
    virtual const char *get_app_name() const = 0;

    ///
    /// \brief set
    ///     store the k-v to the cluster.
    ///     key is composed of hashkey and sortkey.
    /// \param hashkey
    /// used to decide which partition to put this k-v
    /// \param sortkey
    /// all the k-v under hashkey will be sorted by sortkey.
    /// \param value
    /// the value we want to store.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \param ttl_seconds
    /// time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    virtual int set(const std::string &hashkey,
                    const std::string &sortkey,
                    const std::string &value,
                    int timeout_milliseconds = 5000,
                    int ttl_seconds = 0,
                    internal_info *info = NULL) = 0;

    ///
    /// \brief asynchronous set
    ///     store the k-v to the cluster.
    ///     will not be blocked, return immediately.
    ///     key is composed of hashkey and sortkey.
    /// \param hashkey
    /// used to decide which partition to put this k-v
    /// \param sortkey
    /// all the k-v under hashkey will be stored by sortkey.
    /// \param value
    /// the value we want to store.
    /// \param callback
    /// the callback function will be invoked after operation finished or error occurred.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error.
    /// \param ttl_seconds
    /// time to live of this value, if expired, will return not found; 0 means no ttl.
    /// \return
    /// void.
    ///
    virtual void async_set(const std::string &hashkey,
                           const std::string &sortkey,
                           const std::string &value,
                           async_set_callback_t &&callback = nullptr,
                           int timeout_milliseconds = 5000,
                           int ttl_seconds = 0) = 0;

    ///
    /// \brief multi_set (guarantee atomicity)
    ///     store multiple k-v of the same hashkey to the cluster.
    /// \param hashkey
    /// used to decide which partition to put this k-v
    /// \param kvs
    /// all <sortkey,value> pairs to be set. should not be empty
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \param ttl_seconds
    /// time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string().
    /// return PERR_INVALID_ARGUMENT if param kvs is empty.
    ///
    virtual int multi_set(const std::string &hashkey,
                          const std::map<std::string, std::string> &kvs,
                          int timeout_milliseconds = 5000,
                          int ttl_seconds = 0,
                          internal_info *info = NULL) = 0;

    ///
    /// \brief asynchronous multi_set (guarantee atomicity)
    ///     store multiple k-v of the same hashkey to the cluster.
    ///     will not be blocked, return immediately.
    /// \param hashkey
    /// used to decide which partition to put this k-v
    /// \param kvs
    /// all <sortkey,value> pairs to be set. should not be empty
    /// \param callback
    /// the callback function will be invoked after operation finished or error occurred.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \param ttl_seconds
    /// time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    /// void.
    ///
    virtual void async_multi_set(const std::string &hashkey,
                                 const std::map<std::string, std::string> &kvs,
                                 async_multi_set_callback_t &&callback = nullptr,
                                 int timeout_milliseconds = 5000,
                                 int ttl_seconds = 0) = 0;

    ///
    /// \brief get
    ///     get value by key from the cluster.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkey
    /// all the k-v under hashkey will be sorted by sortkey.
    /// \param value
    /// the returned value will be put into it.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string().
    /// returns PERR_NOT_FOUND if no value is found under the <hashkey,sortkey>.
    ///
    virtual int get(const std::string &hashkey,
                    const std::string &sortkey,
                    std::string &value,
                    int timeout_milliseconds = 5000,
                    internal_info *info = NULL) = 0;

    ///
    /// \brief asynchronous get
    ///     get value by key from the cluster.
    ///     will not be blocked, return immediately.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkey
    /// all the k-v under hashkey will be sorted by sortkey.
    /// \param callback
    /// the callback function will be invoked after operation finished or error occurred.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// void.
    ///
    virtual void async_get(const std::string &hashkey,
                           const std::string &sortkey,
                           async_get_callback_t &&callback = nullptr,
                           int timeout_milliseconds = 5000) = 0;

    ///
    /// \brief multi_get
    ///     get multiple value by key from the cluster.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkeys
    /// all the k-v under hashkey will be sorted by sortkey.
    /// if empty, means fetch all sortkeys under the hashkey.
    /// \param values
    /// the returned <sortkey,value> pairs will be put into it.
    /// if data is not found for some <hashkey,sortkey>, then it will not appear in the map.
    /// \param max_fetch_count
    /// max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
    /// \param max_fetch_size
    /// max size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string().
    /// returns PERR_OK if fetch done, even no data is returned.
    /// returns PERR_INCOMPLETE is only partial data is fetched.
    ///
    virtual int multi_get(const std::string &hashkey,
                          const std::set<std::string> &sortkeys,
                          std::map<std::string, std::string> &values,
                          int max_fetch_count = 100,
                          int max_fetch_size = 1000000,
                          int timeout_milliseconds = 5000,
                          internal_info *info = NULL) = 0;

    ///
    /// \brief asynchronous multi_get
    ///     get multiple value by key from the cluster.
    ///     will not be blocked, return immediately.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkeys
    /// all the k-v under hashkey will be sorted by sortkey.
    /// if empty, means fetch all sortkeys under the hashkey.
    /// \param callback
    /// the callback function will be invoked after operation finished or error occurred.
    /// \param max_fetch_count
    /// max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
    /// \param max_fetch_size
    /// max size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// void.
    ///
    virtual void async_multi_get(const std::string &hashkey,
                                 const std::set<std::string> &sortkeys,
                                 async_multi_get_callback_t &&callback = nullptr,
                                 int max_fetch_count = 100,
                                 int max_fetch_size = 1000000,
                                 int timeout_milliseconds = 5000) = 0;

    ///
    /// \brief multi_get
    ///     get multiple value by hash_key and sort_key range from the cluster.
    /// \param hashkey
    /// used to decide which partition to get this k-v.
    /// \param start_sortkey
    /// the start sort key.
    /// \param stop_sortkey
    /// the stop sort key, empty string means fetch until the last.
    /// \param options
    /// the multi-get options.
    /// \param values
    /// the returned <sortkey,value> pairs will be put into it.
    /// \param max_fetch_count
    /// max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
    /// \param max_fetch_size
    /// max size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string().
    /// returns PERR_OK if fetch done, even no data is returned.
    /// returns PERR_INCOMPLETE is only partial data is fetched.
    ///
    virtual int multi_get(const std::string &hashkey,
                          const std::string &start_sortkey,
                          const std::string &stop_sortkey,
                          const multi_get_options &options,
                          std::map<std::string, std::string> &values,
                          int max_fetch_count = 100,
                          int max_fetch_size = 1000000,
                          int timeout_milliseconds = 5000,
                          internal_info *info = NULL) = 0;

    ///
    /// \brief asynchronous multi_get
    ///     get multiple value by hash_key and sort_key range from the cluster.
    ///     will not be blocked, return immediately.
    /// \param hashkey
    /// used to decide which partition to get this k-v.
    /// \param start_sortkey
    /// the start sort key.
    /// \param stop_sortkey
    /// the stop sort key, empty string means fetch until the last.
    /// \param options
    /// the multi-get options.
    /// \param callback
    /// the callback function will be invoked after operation finished or error occurred.
    /// \param max_fetch_count
    /// max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
    /// \param max_fetch_size
    /// max size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// void.
    ///
    virtual void async_multi_get(const std::string &hashkey,
                                 const std::string &start_sortkey,
                                 const std::string &stop_sortkey,
                                 const multi_get_options &options,
                                 async_multi_get_callback_t &&callback = nullptr,
                                 int max_fetch_count = 100,
                                 int max_fetch_size = 1000000,
                                 int timeout_milliseconds = 5000) = 0;

    ///
    /// \brief multi_get_sortkeys
    ///     get multiple sort keys by hash key from the cluster.
    ///     only fetch sort keys, but not fetch values.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkeys
    /// the returned sort keys will be put into it.
    /// \param max_fetch_count
    /// max count of sort keys to be fetched. max_fetch_count <= 0 means no limit.
    /// \param max_fetch_size
    /// max size of sort keys to be fetched. max_fetch_size <= 0 means no limit.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string().
    /// returns PERR_OK if fetch done, even no data is returned.
    /// returns PERR_INCOMPLETE is only partial data is fetched.
    ///
    virtual int multi_get_sortkeys(const std::string &hashkey,
                                   std::set<std::string> &sortkeys,
                                   int max_fetch_count = 100,
                                   int max_fetch_size = 1000000,
                                   int timeout_milliseconds = 5000,
                                   internal_info *info = NULL) = 0;

    ///
    /// \brief asynchronous multi_get_sortkeys
    ///     get multiple sort keys by hash key from the cluster.
    ///     only fetch sort keys, but not fetch values.
    ///     will not be blocked, return immediately.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param callback
    /// the callback function will be invoked after operation finished or error occurred.
    /// \param max_fetch_count
    /// max count of sort keys to be fetched. max_fetch_count <= 0 means no limit.
    /// \param max_fetch_size
    /// max size of sort keys to be fetched. max_fetch_size <= 0 means no limit.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// void.
    ///
    virtual void async_multi_get_sortkeys(const std::string &hashkey,
                                          async_multi_get_sortkeys_callback_t &&callback = nullptr,
                                          int max_fetch_count = 100,
                                          int max_fetch_size = 1000000,
                                          int timeout_milliseconds = 5000) = 0;

    ///
    /// \brief exist
    ///     check value exist by key from the cluster.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkey
    /// all the k-v under hashkey will be sorted by sortkey.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string().
    /// returns PERR_OK if exist.
    /// returns PERR_NOT_FOUND if not exist.
    ///
    virtual int exist(const std::string &hashkey,
                      const std::string &sortkey,
                      int timeout_milliseconds = 5000,
                      internal_info *info = NULL) = 0;

    ///
    /// \brief sortkey_count
    ///     get sortkey count by hashkey from the cluster.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param count
    /// the returned sortkey count
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string().
    ///
    virtual int sortkey_count(const std::string &hashkey,
                              int64_t &count,
                              int timeout_milliseconds = 5000,
                              internal_info *info = NULL) = 0;

    ///
    /// \brief del
    ///     del stored k-v by key from cluster
    ///     key is composed of hashkey and sortkey. must provide both to get the value.
    /// \param hashkey
    /// used to decide from which partition to del this k-v
    /// \param sortkey
    /// all the k-v under hashkey will be sorted by sortkey.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    virtual int del(const std::string &hashkey,
                    const std::string &sortkey,
                    int timeout_milliseconds = 5000,
                    internal_info *info = NULL) = 0;

    ///
    /// \brief asynchronous del
    ///     del stored k-v by key from cluster
    ///     key is composed of hashkey and sortkey. must provide both to get the value.
    ///     will not be blocked, return immediately.
    /// \param hashkey
    /// used to decide from which partition to del this k-v
    /// \param sortkey
    /// all the k-v under hashkey will be sorted by sortkey.
    /// \param callback
    /// the callback function will be invoked after operation finished or error occurred.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// void.
    ///
    virtual void async_del(const std::string &hashkey,
                           const std::string &sortkey,
                           async_del_callback_t &&callback = nullptr,
                           int timeout_milliseconds = 5000) = 0;

    ///
    /// \brief multi_del
    ///     delete multiple value by key from the cluster.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkeys
    /// all the k-v under hashkey will be sorted by sortkey. should not be empty.
    /// \param deleted_count
    /// return count of deleted k-v pairs.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string().
    ///
    virtual int multi_del(const std::string &hashkey,
                          const std::set<std::string> &sortkeys,
                          int64_t &deleted_count,
                          int timeout_milliseconds = 5000,
                          internal_info *info = NULL) = 0;

    ///
    /// \brief asynchronous multi_del
    ///     delete multiple value by key from the cluster.
    ///     will not be blocked, return immediately.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkeys
    /// all the k-v under hashkey will be sorted by sortkey.
    /// if empty, means delete all sortkeys under the hashkey.
    /// \param callback
    /// the callback function will be invoked after operation finished or error occurred.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// void.
    ///
    virtual void async_multi_del(const std::string &hashkey,
                                 const std::set<std::string> &sortkeys,
                                 async_multi_del_callback_t &&callback = nullptr,
                                 int timeout_milliseconds = 5000) = 0;

    ///
    /// \brief ttl (time to live)
    ///     get ttl in seconds of this k-v.
    ///     key is composed of hashkey and sortkey. must provide both to get the value.
    /// \param hashkey
    /// used to decide which partition to get this k-v
    /// \param sortkey
    /// all the k-v under hashkey will be sorted by sortkey.
    /// \param ttl_seconds
    /// the returned ttl value in seconds.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    virtual int ttl(const std::string &hashkey,
                    const std::string &sortkey,
                    int &ttl_seconds,
                    int timeout_milliseconds = 5000,
                    internal_info *info = NULL) = 0;

    ///
    /// \brief get hash scanner
    ///     get scanner for [start_sortkey, stop_sortkey) of hashkey
    /// \param hashkey
    /// cannot be empty
    /// \param start_sortkey
    /// sortkey to start with
    /// \param stop_sortkey
    /// sortkey to stop. ""(empty string) represents the max key
    /// \param options
    /// which used to indicate scan options, like which bound is inclusive
    /// \param scanner
    /// out param, used to get k-v
    /// this pointer should be deleted when scan complete
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    virtual int get_scanner(const std::string &hashkey,
                            const std::string &start_sortkey, // start from beginning if this set ""
                            const std::string &stop_sortkey,  // to the last item if this set ""
                            const scan_options &options,
                            pegasus_scanner *&scanner) = 0;

    ///
    /// \brief async get hash scanner
    ///     get scanner for [start_sortkey, stop_sortkey) of hashkey
    ///     will not be blocked, return immediately.
    /// \param hashkey
    /// cannot be empty
    /// \param start_sortkey
    /// sortkey to start with
    /// \param stop_sortkey
    /// sortkey to stop. ""(empty string) represents the max key
    /// \param options
    /// which used to indicate scan options, like which bound is inclusive
    /// \param callback
    /// return status and scanner in callback, and the latter should be deleted when scan complete
    ///
    virtual void
    async_get_scanner(const std::string &hashkey,
                      const std::string &start_sortkey, // start from beginning if this set ""
                      const std::string &stop_sortkey,  // to the last item if this set ""
                      const scan_options &options,
                      async_get_scanner_callback_t &&callback) = 0;

    ///
    /// \brief get a bundle of scanners to iterate all k-v in table
    ///        scanners should be deleted when scan complete
    /// \param max_split_count
    /// the number of scanners returned will always <= max_split_count
    /// \param options
    /// which used to indicate scan options, like timeout_milliseconds
    /// \param scanners
    /// out param, used to get k-v
    /// these pointers should be deleted
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    virtual int get_unordered_scanners(int max_split_count,
                                       const scan_options &options,
                                       std::vector<pegasus_scanner *> &scanners) = 0;

    ///
    /// \brief async get a bundle of scanners to iterate all k-v in table
    ///        scannners return by callback should be deleted when all scan complete
    /// \param max_split_count
    /// the number of scanners returned will always <= max_split_count
    /// \param options
    /// which used to indicate scan options, like timeout_milliseconds
    /// \param callback; return status and scanner in this callback
    ///
    virtual void
    async_get_unordered_scanners(int max_split_count,
                                 const scan_options &options,
                                 async_get_unordered_scanners_callback_t &&callback) = 0;

    ///
    /// \brief get_error_string
    /// get error string
    /// all the function above return an int value that indicates an error can be converted into a
    /// string for human reading.
    /// \param error_code
    /// all the error code are defined in "error_def.h"
    /// \return
    ///
    virtual const char *get_error_string(int error_code) const = 0;
};

class pegasus_client_factory
{
public:
    ///
    /// \brief initialize
    /// initialize pegasus client lib. must call this function before anything else.
    /// \param config_file
    /// the configuration file of client lib
    /// \return
    /// true indicate the initailize is success.
    ///
    static bool initialize(const char *config_file);

    ///
    /// \brief get_client
    /// get an instance for a given cluster and a given app name.
    /// \param cluster_name
    /// the pegasus cluster name.
    /// a cluster can have multiple apps.
    /// \param app_name
    /// an app is a logical isolated k-v store.
    /// a cluster can have multiple apps.
    /// \return
    /// the client instance. DO NOT delete this client even after usage.
    static pegasus_client *get_client(const char *cluster_name, const char *app_name);
};

} // namespace
