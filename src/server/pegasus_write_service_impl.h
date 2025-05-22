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

#pragma once

#include <gtest/gtest_prod.h>

#include "base/idl_utils.h"
#include "base/meta_store.h"
#include "base/pegasus_key_schema.h"
#include "logging_utils.h"
#include "pegasus_server_impl.h"
#include "pegasus_write_service.h"
#include "rocksdb_wrapper.h"
#include "utils/defer.h"
#include "utils/env.h"
#include "utils/filesystem.h"
#include "utils/string_conv.h"
#include "utils/strings.h"

namespace pegasus::server {

/// internal error codes used for fail injection
// TODO(yingchun): Use real rocksdb::Status::code.
static constexpr int FAIL_DB_WRITE_BATCH_PUT = -101;
static constexpr int FAIL_DB_WRITE_BATCH_DELETE = -102;
static constexpr int FAIL_DB_WRITE = -103;
static constexpr int FAIL_DB_GET = -104;

struct db_get_context
{
    // value read from DB.
    std::string raw_value;

    // is the record found in DB.
    bool found{false};

    // the expiration time encoded in raw_value.
    uint32_t expire_ts{0};

    // is the record expired.
    bool expired{false};
};

inline dsn::error_code get_external_files_path(const std::string &bulk_load_dir,
                                               const bool verify_before_ingest,
                                               const dsn::replication::bulk_load_metadata &metadata,
                                               /*out*/ std::vector<std::string> &files_path)
{
    for (const auto &f_meta : metadata.files) {
        const auto &file_name = dsn::utils::filesystem::path_combine(bulk_load_dir, f_meta.name);
        if (verify_before_ingest &&
            !dsn::utils::filesystem::verify_file(
                file_name, dsn::utils::FileDataType::kSensitive, f_meta.md5, f_meta.size)) {
            break;
        }
        files_path.emplace_back(file_name);
    }
    return files_path.size() == metadata.files.size() ? dsn::ERR_OK : dsn::ERR_WRONG_CHECKSUM;
}

class pegasus_write_service::impl : public dsn::replication::replica_base
{
public:
    explicit impl(pegasus_server_impl *server)
        : replica_base(server),
          _primary_host_port(server->_primary_host_port),
          _pegasus_data_version(server->_pegasus_data_version)
    {
        _rocksdb_wrapper = std::make_unique<rocksdb_wrapper>(server);
    }

    int empty_put(int64_t decree)
    {
        int err =
            _rocksdb_wrapper->write_batch_put(decree, std::string_view(), std::string_view(), 0);
        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        if (err != rocksdb::Status::kOk) {
            return err;
        }

        return _rocksdb_wrapper->write(decree);
    }

    int multi_put(const db_write_context &ctx,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp)
    {
        int64_t decree = ctx.decree;
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_host_port;

        if (update.kvs.empty()) {
            LOG_ERROR_PREFIX("invalid argument for multi_put: decree = {}, error = {}",
                             decree,
                             "request.kvs is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        for (const auto &kv : update.kvs) {
            resp.error =
                _rocksdb_wrapper->write_batch_put_ctx(ctx,
                                                      composite_raw_key(update.hash_key, kv.key),
                                                      kv.value,
                                                      update.expire_ts_seconds);
            if (dsn_unlikely(resp.error != rocksdb::Status::kOk)) {
                return resp.error;
            }
        }

        resp.error = _rocksdb_wrapper->write(decree);
        return resp.error;
    }

    int multi_remove(int64_t decree,
                     const dsn::apps::multi_remove_request &update,
                     dsn::apps::multi_remove_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_host_port;

        if (update.sort_keys.empty()) {
            LOG_ERROR_PREFIX("invalid argument for multi_remove: decree = {}, error = {}",
                             decree,
                             "request.sort_keys is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        for (const auto &sort_key : update.sort_keys) {
            resp.error = _rocksdb_wrapper->write_batch_delete(
                decree, composite_raw_key(update.hash_key, sort_key).to_string_view());
            if (dsn_unlikely(resp.error != rocksdb::Status::kOk)) {
                return resp.error;
            }
        }

        resp.error = _rocksdb_wrapper->write(decree);
        if (resp.error == rocksdb::Status::kOk) {
            resp.count = update.sort_keys.size();
        }
        return resp.error;
    }

    // Tranlate an incr request into a single-put request which is certainly idempotent.
    // Return current status for RocksDB. Only called by primary replicas.
    int make_idempotent(const dsn::apps::incr_request &req,
                        dsn::apps::incr_response &err_resp,
                        dsn::apps::update_request &update)
    {
        // Get current raw value for the provided key from the RocksDB instance.
        db_get_context get_ctx;
        const int err = _rocksdb_wrapper->get(req.key, &get_ctx);
        if (dsn_unlikely(err != rocksdb::Status::kOk)) {
            // Failed to read current raw value.
            LOG_ERROR_PREFIX("failed to get current raw value for incr while making "
                             "idempotent: rocksdb_status = {}, key = {}",
                             err,
                             utils::c_escape_sensitive_string(req.key));

            return make_error_response(err, err_resp);
        }

        if (!get_ctx.found || get_ctx.expired) {
            // Once the provided key is not found or has been expired, we could assume that
            // its value is 0 before incr; thus the final result for incr could be set as
            // the value of the single-put request, i.e. req.increment.
            return make_idempotent_request_for_incr(
                req.key, req.increment, calc_expire_on_non_existent(req), update);
        }

        // Extract user data from raw value as base for increment.
        dsn::blob base_value;
        pegasus_extract_user_data(_pegasus_data_version, std::move(get_ctx.raw_value), base_value);

        int64_t new_int = 0;
        if (base_value.empty()) {
            // Old value is also considered as 0 before incr as above once it's empty, thus
            // set req.increment as the value for single put.
            new_int = req.increment;
        } else {
            int64_t base_int = 0;
            if (dsn_unlikely(!dsn::buf2int64(base_value.to_string_view(), base_int))) {
                // Old value is not valid int64.
                LOG_ERROR_PREFIX("incr failed: error = base value \"{}\" "
                                 "is not an integer or out of range",
                                 utils::c_escape_sensitive_string(base_value));
                return make_error_response(rocksdb::Status::kInvalidArgument, err_resp);
            }

            new_int = base_int + req.increment;
            if (dsn_unlikely((req.increment > 0 && new_int < base_int) ||
                             (req.increment < 0 && new_int > base_int))) {
                // New value overflows, just respond with the base value.
                LOG_ERROR_PREFIX("incr failed: error = new value is out of range, "
                                 "base_value = {}, increment = {}, new_value = {}",
                                 base_int,
                                 req.increment,
                                 new_int);
                return make_error_response(rocksdb::Status::kInvalidArgument, base_int, err_resp);
            }
        }

        // Generate the idempotent single-put request for incr.
        return make_idempotent_request_for_incr(
            req.key, new_int, calc_expire_on_existing(req, get_ctx), update);
    }

    // Apply the single-put request translated from an incr request into RocksDB, and build
    // response for the incr request. Return current status for RocksDB. Only called by
    // primary replicas.
    int put(const db_write_context &ctx,
            const dsn::apps::update_request &update,
            dsn::apps::incr_response &resp)
    {
        make_basic_response(ctx.decree, resp);

        const auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });

        resp.error = _rocksdb_wrapper->write_batch_put_ctx(
            ctx, update.key, update.value, update.expire_ts_seconds);
        if (dsn_unlikely(resp.error != rocksdb::Status::kOk)) {
            return resp.error;
        }

        resp.error = _rocksdb_wrapper->write(ctx.decree);
        if (dsn_unlikely(resp.error != rocksdb::Status::kOk)) {
            return resp.error;
        }

        // Shouldn't fail to parse since the value must be a valid int64.
        CHECK(dsn::buf2int64(update.value.to_string_view(), resp.new_value),
              "invalid int64 value for put idempotent incr: key={}, value={}",
              update.key,
              update.value);

        return rocksdb::Status::kOk;
    }

    int incr(int64_t decree, const dsn::apps::incr_request &update, dsn::apps::incr_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_host_port;

        std::string_view raw_key = update.key.to_string_view();
        int64_t new_value = 0;
        uint32_t new_expire_ts = 0;
        db_get_context get_ctx;
        int err = _rocksdb_wrapper->get(raw_key, &get_ctx);
        if (err != rocksdb::Status::kOk) {
            resp.error = err;
            return err;
        }
        if (!get_ctx.found) {
            // old value is not found, set to 0 before increment
            new_value = update.increment;
            new_expire_ts = update.expire_ts_seconds > 0 ? update.expire_ts_seconds : 0;
        } else if (get_ctx.expired) {
            // ttl timeout, set to 0 before increment
            new_value = update.increment;
            new_expire_ts = update.expire_ts_seconds > 0 ? update.expire_ts_seconds : 0;
        } else {
            ::dsn::blob old_value;
            pegasus_extract_user_data(
                _pegasus_data_version, std::move(get_ctx.raw_value), old_value);
            if (old_value.length() == 0) {
                // empty old value, set to 0 before increment
                new_value = update.increment;
            } else {
                int64_t old_value_int;
                if (!dsn::buf2int64(old_value.to_string_view(), old_value_int)) {
                    // invalid old value
                    LOG_ERROR_PREFIX("incr failed: decree = {}, error = "
                                     "old value \"{}\" is not an integer or out of range",
                                     decree,
                                     utils::c_escape_sensitive_string(old_value));
                    resp.error = rocksdb::Status::kInvalidArgument;
                    // we should write empty record to update rocksdb's last flushed decree
                    return empty_put(decree);
                }
                new_value = old_value_int + update.increment;
                if ((update.increment > 0 && new_value < old_value_int) ||
                    (update.increment < 0 && new_value > old_value_int)) {
                    // new value is out of range, return old value by 'new_value'
                    LOG_ERROR_PREFIX("incr failed: decree = {}, error = "
                                     "new value is out of range, old_value = {}, increment = {}",
                                     decree,
                                     old_value_int,
                                     update.increment);
                    resp.error = rocksdb::Status::kInvalidArgument;
                    resp.new_value = old_value_int;
                    // we should write empty record to update rocksdb's last flushed decree
                    return empty_put(decree);
                }
            }
            // set new ttl
            if (update.expire_ts_seconds == 0) {
                new_expire_ts = get_ctx.expire_ts;
            } else if (update.expire_ts_seconds < 0) {
                new_expire_ts = 0;
            } else { // update.expire_ts_seconds > 0
                new_expire_ts = update.expire_ts_seconds;
            }
        }

        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        resp.error = _rocksdb_wrapper->write_batch_put(
            decree, update.key.to_string_view(), std::to_string(new_value), new_expire_ts);
        if (resp.error != rocksdb::Status::kOk) {
            return resp.error;
        }

        resp.error = _rocksdb_wrapper->write(decree);
        if (resp.error == rocksdb::Status::kOk) {
            resp.new_value = new_value;
        }

        return resp.error;
    }

    // Tranlate a check_and_set request into a single-put request which is certainly idempotent.
    // Return current status for RocksDB. Only called by primary replicas.
    int make_idempotent(const dsn::apps::check_and_set_request &req,
                        dsn::apps::check_and_set_response &err_resp,
                        dsn::apps::update_request &update)
    {
        if (dsn_unlikely(!is_check_type_supported(req.check_type))) {
            LOG_ERROR_PREFIX("check type {} is not supported for check_and_set ",
                             "while making idempotent",
                             cas_check_type_to_string(req.check_type));

            return make_error_response(rocksdb::Status::kInvalidArgument, err_resp);
        }

        dsn::blob check_key;
        pegasus_generate_key(check_key, req.hash_key, req.check_sort_key);

        // Get the check value.
        db_get_context get_ctx;
        const int err = _rocksdb_wrapper->get(check_key, &get_ctx);
        if (dsn_unlikely(err != rocksdb::Status::kOk)) {
            // Failed to read the check value.
            LOG_ERROR_PREFIX("failed to get the check value for check_and_set while making "
                             "idempotent: rocksdb_status = {}, hash_key = {}, "
                             "check_sort_key = {}",
                             err,
                             utils::c_escape_sensitive_string(req.hash_key),
                             utils::c_escape_sensitive_string(req.check_sort_key));

            return make_error_response(err, err_resp);
        }

        dsn::blob check_value;
        const bool value_exist = !get_ctx.expired && get_ctx.found;
        if (value_exist) {
            pegasus_extract_user_data(
                _pegasus_data_version, std::move(get_ctx.raw_value), check_value);
        }

        bool invalid_argument = false;
        const bool passed = validate_check(
            req.check_type, req.check_operand, value_exist, check_value, invalid_argument);
        if (!passed) {
            make_check_value(req, value_exist, check_value, err_resp);
            return make_error_response(invalid_argument ? rocksdb::Status::kInvalidArgument
                                                        : rocksdb::Status::kTryAgain,
                                       err_resp);
        }

        // Check passed, generate new RocksDB key.
        dsn::blob set_key;
        if (req.set_diff_sort_key) {
            pegasus_generate_key(set_key, req.hash_key, req.set_sort_key);
        } else {
            set_key = check_key;
        }

        // Generate new RocksDB value.
        make_check_value(req, value_exist, check_value, update);

        // Generate the idempotent single-put request for check_and_set.
        return make_idempotent_request_for_check_and_set(
            set_key, req.set_value, req.set_expire_ts_seconds, update);
    }

    // Apply the single-put request translated from a check_and_set request into RocksDB, and
    // build response for the check_and_set request. Return current status for RocksDB. Only
    // called by primary replicas.
    int put(const db_write_context &ctx,
            const dsn::apps::update_request &update,
            dsn::apps::check_and_set_response &resp)
    {
        make_basic_response(ctx.decree, resp);

        // Copy check_value's fields from the single-put request to the check_and_set
        // response to reply to the client.
        copy_check_value(update, resp);

        const auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });

        resp.error = _rocksdb_wrapper->write_batch_put_ctx(
            ctx, update.key, update.value, update.expire_ts_seconds);
        if (dsn_unlikely(resp.error != rocksdb::Status::kOk)) {
            return resp.error;
        }

        resp.error = _rocksdb_wrapper->write(ctx.decree);
        return resp.error;
    }

    int check_and_set(int64_t decree,
                      const dsn::apps::check_and_set_request &update,
                      dsn::apps::check_and_set_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_host_port;

        if (!is_check_type_supported(update.check_type)) {
            LOG_ERROR_PREFIX("invalid argument for check_and_set: decree = {}, error = {}",
                             decree,
                             fmt::format("check type {} not supported", update.check_type));
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        ::dsn::blob check_key;
        pegasus_generate_key(check_key, update.hash_key, update.check_sort_key);

        db_get_context get_context;
        std::string_view check_raw_key = check_key.to_string_view();
        const int err = _rocksdb_wrapper->get(check_raw_key, &get_context);
        if (dsn_unlikely(err != rocksdb::Status::kOk)) {
            // read check value failed
            LOG_ERROR_PREFIX("Error to GetCheckValue for CheckAndSet decree: {}, hash_key: {}, "
                             "check_sort_key: {}",
                             decree,
                             utils::c_escape_sensitive_string(update.hash_key),
                             utils::c_escape_sensitive_string(update.check_sort_key));
            resp.error = err;
            return resp.error;
        }

        ::dsn::blob check_value;
        bool value_exist = !get_context.expired && get_context.found;
        if (value_exist) {
            pegasus_extract_user_data(
                _pegasus_data_version, std::move(get_context.raw_value), check_value);
        }

        if (update.return_check_value) {
            resp.check_value_returned = true;
            if (value_exist) {
                resp.check_value_exist = true;
                resp.check_value = check_value;
            }
        }

        bool invalid_argument = false;
        bool passed = validate_check(decree,
                                     update.check_type,
                                     update.check_operand,
                                     value_exist,
                                     check_value,
                                     invalid_argument);

        if (passed) {
            // check passed, write new value
            ::dsn::blob set_key;
            if (update.set_diff_sort_key) {
                pegasus_generate_key(set_key, update.hash_key, update.set_sort_key);
            } else {
                set_key = check_key;
            }
            resp.error = _rocksdb_wrapper->write_batch_put(
                decree,
                set_key.to_string_view(),
                update.set_value.to_string_view(),
                static_cast<uint32_t>(update.set_expire_ts_seconds));
        } else {
            // check not passed, write empty record to update rocksdb's last flushed decree
            resp.error = _rocksdb_wrapper->write_batch_put(
                decree, std::string_view(), std::string_view(), 0);
        }

        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        if (resp.error) {
            return resp.error;
        }

        resp.error = _rocksdb_wrapper->write(decree);
        if (resp.error) {
            return resp.error;
        }

        if (!passed) {
            // check not passed, return proper error code to user
            resp.error = invalid_argument ? rocksdb::Status::kInvalidArgument
                                          : rocksdb::Status::kTryAgain;
        }

        return rocksdb::Status::kOk;
    }

    // Calling make_idempotent() on either an incr or check_and_set operation produces an
    // idempotent single-put request, which is also the only element in the output array
    // `updates`.
    //
    // This interface is provided to ensure consistency between the make_idempotent() interfaces
    // of incr/check_and_set operations and that of check_and_mutate (both using std::vector
    // for `updates`), thereby facilitating uniform templated function invocation.
    template <typename TRequest, typename TResponse>
    inline int make_idempotent(const TRequest &req,
                               TResponse &err_resp,
                               std::vector<dsn::apps::update_request> &updates)
    {
        updates.clear();
        updates.emplace_back();
        return make_idempotent(req, err_resp, updates.front());
    }

    // Apply `updates` into storage engine. Actually `updates` contains only one element --
    // an idempotent single-put request translated from either an incr or check_and_set
    // operation.
    //
    // This interface is provided to ensure consistency between the put() interfaces of
    // incr/check_and_set operations and that of check_and_mutate (both using std::vector
    // for `updates`), thereby facilitating uniform templated function invocation.
    template <typename TResponse>
    int put(const db_write_context &ctx,
            const std::vector<dsn::apps::update_request> &updates,
            TResponse &resp)
    {
        CHECK_EQ(updates.size(), 1);
        return put(ctx, updates.front(), resp);
    }

    // Tranlate a check_and_mutate request into multiple single-put and single-remove requests
    // which are certainly idempotent. Return current status for RocksDB. Only called by primary
    // replicas.
    int make_idempotent(const dsn::apps::check_and_mutate_request &req,
                        dsn::apps::check_and_mutate_response &err_resp,
                        std::vector<dsn::apps::update_request> &updates)
    {
        if (dsn_unlikely(req.mutate_list.empty())) {
            LOG_ERROR_PREFIX("mutate_list is empty for check_and_mutate while making idempotent");

            return make_error_response(rocksdb::Status::kInvalidArgument, err_resp);
        }

        // Verify operation type for each mutate.
        for (size_t i = 0; i < req.mutate_list.size(); ++i) {
            const auto &mu = req.mutate_list[i];
            if (dsn_likely(mu.operation == dsn::apps::mutate_operation::MO_PUT ||
                           mu.operation == dsn::apps::mutate_operation::MO_DELETE)) {
                continue;
            }

            LOG_ERROR_PREFIX("mutate_list[{}]'s operation {} is invalid for check_and_mutate "
                             "while making idempotent",
                             i,
                             mu.operation);

            return make_error_response(rocksdb::Status::kInvalidArgument, err_resp);
        }

        if (dsn_unlikely(!is_check_type_supported(req.check_type))) {
            LOG_ERROR_PREFIX("check type {} is not supported for check_and_mutate ",
                             "while making idempotent",
                             cas_check_type_to_string(req.check_type));

            return make_error_response(rocksdb::Status::kInvalidArgument, err_resp);
        }

        dsn::blob check_key;
        pegasus_generate_key(check_key, req.hash_key, req.check_sort_key);

        // Get the check value.
        db_get_context get_ctx;
        const int err = _rocksdb_wrapper->get(check_key, &get_ctx);
        if (dsn_unlikely(err != rocksdb::Status::kOk)) {
            // Failed to read the check value.
            LOG_ERROR_PREFIX("failed to get the check value for check_and_mutate while making "
                             "idempotent: rocksdb_status = {}, hash_key = {}, "
                             "check_sort_key = {}",
                             err,
                             utils::c_escape_sensitive_string(req.hash_key),
                             utils::c_escape_sensitive_string(req.check_sort_key));

            return make_error_response(err, err_resp);
        }

        dsn::blob check_value;
        const bool value_exist = !get_ctx.expired && get_ctx.found;
        if (value_exist) {
            pegasus_extract_user_data(
                _pegasus_data_version, std::move(get_ctx.raw_value), check_value);
        }

        bool invalid_argument = false;
        const bool passed = validate_check(
            req.check_type, req.check_operand, value_exist, check_value, invalid_argument);
        if (!passed) {
            make_check_value(req, value_exist, check_value, err_resp);
            return make_error_response(invalid_argument ? rocksdb::Status::kInvalidArgument
                                                        : rocksdb::Status::kTryAgain,
                                       err_resp);
        }

        // Check passed.
        updates.clear();
        for (const auto &mu : req.mutate_list) {
            // Generate new RocksDB key.
            dsn::blob set_key;
            pegasus_generate_key(set_key, req.hash_key, mu.sort_key);

            // Add a new put request.
            updates.emplace_back();

            if (mu.operation == dsn::apps::mutate_operation::MO_PUT) {
                make_idempotent_request_for_check_and_mutate_put(
                    set_key, mu.value, mu.set_expire_ts_seconds, updates.back());
                continue;
            }

            if (mu.operation == dsn::apps::mutate_operation::MO_DELETE) {
                make_idempotent_request_for_check_and_mutate_remove(set_key, updates.back());
                continue;
            }

            // It must have returned and replied to the client once there is some invalid
            // mutate_operation. Here is just a defensive assertion.
            LOG_FATAL("invalid mutate_operation {} for check_and_mutate while making idempotent",
                      mu.operation);
            __builtin_unreachable();
        }

        // Add check value to the first generated idempotent request, for the future response to
        // the client.
        make_check_value(req, value_exist, check_value, updates.front());

        return rocksdb::Status::kOk;
    }

    // Apply the single-put and single-remove requests translated from a check_and_mutate request
    // into RocksDB, and build response for the check_and_mutate request. Return current status
    // for RocksDB. Only called by primary replicas.
    int put(const db_write_context &ctx,
            const std::vector<dsn::apps::update_request> &updates,
            dsn::apps::check_and_mutate_response &resp)
    {
        make_basic_response(ctx.decree, resp);

        // Copy check_value's fields from the first idempotent request to the check_and_mutate
        // response to reply to the client.
        copy_check_value(updates.front(), resp);

        const auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });

        for (const auto &update : updates) {
            if (update.type == dsn::apps::update_type::UT_CHECK_AND_MUTATE_PUT) {
                resp.error = _rocksdb_wrapper->write_batch_put_ctx(
                    ctx, update.key, update.value, update.expire_ts_seconds);
            } else if (update.type == dsn::apps::update_type::UT_CHECK_AND_MUTATE_REMOVE) {
                resp.error = _rocksdb_wrapper->write_batch_delete(ctx.decree, update.key);
            } else {
                LOG_FATAL("invalid update_type for check_and_mutate {} while making idempotent",
                          update.type);
                __builtin_unreachable();
            }

            if (dsn_unlikely(resp.error != rocksdb::Status::kOk)) {
                return resp.error;
            }
        }

        resp.error = _rocksdb_wrapper->write(ctx.decree);
        return resp.error;
    }

    int check_and_mutate(int64_t decree,
                         const dsn::apps::check_and_mutate_request &update,
                         dsn::apps::check_and_mutate_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_host_port;

        if (update.mutate_list.empty()) {
            LOG_ERROR_PREFIX("invalid argument for check_and_mutate: decree = {}, error = {}",
                             decree,
                             "mutate list is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        for (int i = 0; i < update.mutate_list.size(); ++i) {
            auto &mu = update.mutate_list[i];
            if (mu.operation != ::dsn::apps::mutate_operation::MO_PUT &&
                mu.operation != ::dsn::apps::mutate_operation::MO_DELETE) {
                LOG_ERROR_PREFIX("invalid argument for check_and_mutate: decree = {}, error = "
                                 "mutation[{}] uses invalid operation {}",
                                 decree,
                                 i,
                                 mu.operation);
                resp.error = rocksdb::Status::kInvalidArgument;
                // we should write empty record to update rocksdb's last flushed decree
                return empty_put(decree);
            }
        }

        if (!is_check_type_supported(update.check_type)) {
            LOG_ERROR_PREFIX("invalid argument for check_and_mutate: decree = {}, error = {}",
                             decree,
                             fmt::format("check type {} not supported", update.check_type));
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        ::dsn::blob check_key;
        pegasus_generate_key(check_key, update.hash_key, update.check_sort_key);

        db_get_context get_context;
        std::string_view check_raw_key = check_key.to_string_view();
        int err = _rocksdb_wrapper->get(check_raw_key, &get_context);
        if (err != rocksdb::Status::kOk) {
            // read check value failed
            LOG_ERROR_ROCKSDB("Error to GetCheckValue for CheckAndMutate decree: {}, hash_key: {}, "
                              "check_sort_key: {}",
                              decree,
                              utils::c_escape_sensitive_string(update.hash_key),
                              utils::c_escape_sensitive_string(update.check_sort_key));
            resp.error = err;
            return resp.error;
        }

        ::dsn::blob check_value;
        bool value_exist = !get_context.expired && get_context.found;
        if (value_exist) {
            pegasus_extract_user_data(
                _pegasus_data_version, std::move(get_context.raw_value), check_value);
        }

        if (update.return_check_value) {
            resp.check_value_returned = true;
            if (value_exist) {
                resp.check_value_exist = true;
                resp.check_value = check_value;
            }
        }

        bool invalid_argument = false;
        bool passed = validate_check(decree,
                                     update.check_type,
                                     update.check_operand,
                                     value_exist,
                                     check_value,
                                     invalid_argument);

        if (passed) {
            for (auto &m : update.mutate_list) {
                ::dsn::blob key;
                pegasus_generate_key(key, update.hash_key, m.sort_key);
                if (m.operation == ::dsn::apps::mutate_operation::MO_PUT) {
                    resp.error = _rocksdb_wrapper->write_batch_put(
                        decree,
                        key.to_string_view(),
                        m.value.to_string_view(),
                        static_cast<uint32_t>(m.set_expire_ts_seconds));
                } else {
                    CHECK_EQ(m.operation, ::dsn::apps::mutate_operation::MO_DELETE);
                    resp.error = _rocksdb_wrapper->write_batch_delete(decree, key.to_string_view());
                }

                // in case of failure, cancel mutations
                if (resp.error)
                    break;
            }
        } else {
            // check not passed, write empty record to update rocksdb's last flushed decree
            resp.error = _rocksdb_wrapper->write_batch_put(
                decree, std::string_view(), std::string_view(), 0);
        }

        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        if (resp.error) {
            return resp.error;
        }

        resp.error = _rocksdb_wrapper->write(decree);
        if (resp.error) {
            return resp.error;
        }

        if (!passed) {
            // check not passed, return proper error code to user
            resp.error = invalid_argument ? rocksdb::Status::kInvalidArgument
                                          : rocksdb::Status::kTryAgain;
        }
        return rocksdb::Status::kOk;
    }

    // \return ERR_INVALID_VERSION: replay or commit out-date ingest request
    // \return ERR_WRONG_CHECKSUM: verify files failed
    // \return ERR_INGESTION_FAILED: rocksdb ingestion failed
    // \return ERR_OK: rocksdb ingestion succeed
    dsn::error_code ingest_files(const int64_t decree,
                                 const std::string &bulk_load_dir,
                                 const dsn::replication::ingestion_request &req,
                                 const int64_t current_ballot)
    {
        const auto &req_ballot = req.ballot;

        // if ballot updated, ignore this request
        if (req_ballot < current_ballot) {
            LOG_WARNING_PREFIX("out-dated ingestion request, ballot changed, request({}) vs "
                               "current({}), ignore it",
                               req_ballot,
                               current_ballot);
            return dsn::ERR_INVALID_VERSION;
        }

        // verify external files before ingestion
        std::vector<std::string> sst_file_list;
        const auto &err = get_external_files_path(
            bulk_load_dir, req.verify_before_ingest, req.metadata, sst_file_list);
        if (err != dsn::ERR_OK) {
            return err;
        }

        // ingest external files
        if (dsn_unlikely(_rocksdb_wrapper->ingest_files(decree, sst_file_list, req.ingest_behind) !=
                         rocksdb::Status::kOk)) {
            return dsn::ERR_INGESTION_FAILED;
        }
        return dsn::ERR_OK;
    }

    /// For batch write.

    int batch_put(const db_write_context &ctx,
                  const dsn::apps::update_request &update,
                  dsn::apps::update_response &resp)
    {
        resp.error = _rocksdb_wrapper->write_batch_put_ctx(
            ctx, update.key, update.value, update.expire_ts_seconds);
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp)
    {
        resp.error = _rocksdb_wrapper->write_batch_delete(decree, key.to_string_view());
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_commit(int64_t decree)
    {
        int err = _rocksdb_wrapper->write(decree);
        clear_up_batch_states(decree, err);
        return err;
    }

    void batch_abort(int64_t decree, int err) { clear_up_batch_states(decree, err); }

    void set_default_ttl(uint32_t ttl) { _rocksdb_wrapper->set_default_ttl(ttl); }

private:
    void clear_up_batch_states(int64_t decree, int err)
    {
        if (!_update_responses.empty()) {
            dsn::apps::update_response resp;
            resp.error = err;
            resp.app_id = get_gpid().get_app_id();
            resp.partition_index = get_gpid().get_partition_index();
            resp.decree = decree;
            resp.server = _primary_host_port;
            for (dsn::apps::update_response *uresp : _update_responses) {
                *uresp = resp;
            }
            _update_responses.clear();
        }

        _rocksdb_wrapper->clear_up_write_batch();
    }

    // Convenient encapsulation of pegasus_generate_key().
    //
    // TKey may be std::string_view, std::string or dsn::blob.
    template <typename TKey>
    static dsn::blob composite_raw_key(const TKey &hash_key, const TKey &sort_key)
    {
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, hash_key, sort_key);
        return raw_key;
    }

    // Calculate expire timestamp in seconds for the keys not contained in the storage
    // according to `req`.
    template <typename TRequest>
    static inline int32_t calc_expire_on_non_existent(const TRequest &req)
    {
        return req.expire_ts_seconds > 0 ? req.expire_ts_seconds : 0;
    }

    // Calculate new expire timestamp in seconds for the keys contained in the storage
    // according to `req` and their current expire timestamp in `get_ctx`.
    template <typename TRequest>
    static inline int32_t calc_expire_on_existing(const TRequest &req,
                                                  const db_get_context &get_ctx)
    {
        if (req.expire_ts_seconds == 0) {
            // Still use current expire timestamp of the existing key as the new value.
            return static_cast<int32_t>(get_ctx.expire_ts);
        }

        if (req.expire_ts_seconds < 0) {
            // Reset expire timestamp to 0.
            return 0;
        }

        return req.expire_ts_seconds;
    }

    // Build a single-put request if only `key` and `type` are provided.
    static inline void make_idempotent_request(const dsn::blob &key,
                                               dsn::apps::update_type::type type,
                                               dsn::apps::update_request &update)
    {
        update.key = key;

        // Must use __set_type() since type is declared as optional.
        update.__set_type(type);
    }

    // Build a single-put request while `key`, `type` and `expire_ts_seconds` are provided.
    static inline void make_idempotent_request(const dsn::blob &key,
                                               int32_t expire_ts_seconds,
                                               dsn::apps::update_type::type type,
                                               dsn::apps::update_request &update)
    {
        make_idempotent_request(key, type, update);
        update.expire_ts_seconds = expire_ts_seconds;
    }

    // Build a single-put request while `key`, `type`, `expire_ts_seconds` and int64-typed
    // `value` are provided.
    static inline void make_idempotent_request(const dsn::blob &key,
                                               int64_t value,
                                               int32_t expire_ts_seconds,
                                               dsn::apps::update_type::type type,
                                               dsn::apps::update_request &update)
    {
        make_idempotent_request(key, expire_ts_seconds, type, update);
        update.value = dsn::blob::create_from_numeric(value);
    }

    // Build a single-put request while `key`, `type`, `expire_ts_seconds` and blob-typed
    // `value` are provided.
    static inline void make_idempotent_request(const dsn::blob &key,
                                               const dsn::blob &value,
                                               int32_t expire_ts_seconds,
                                               dsn::apps::update_type::type type,
                                               dsn::apps::update_request &update)
    {
        make_idempotent_request(key, expire_ts_seconds, type, update);
        update.value = value;
    }

    // Build `update` (i.e. the corresponding single-put request for an incr request) based on
    // `key`, `value` and `expire_ts_seconds`. Return current status for RocksDB (i.e. kOk).
    static inline int make_idempotent_request_for_incr(const dsn::blob &key,
                                                       int64_t value,
                                                       int32_t expire_ts_seconds,
                                                       dsn::apps::update_request &update)
    {
        make_idempotent_request(
            key, value, expire_ts_seconds, dsn::apps::update_type::UT_INCR, update);
        return rocksdb::Status::kOk;
    }

    // Set check_value's fields in `resp` based on `req`, `value_exist` and `check_value`.
    //
    // `req` may be dsn::apps::check_and_set_request and dsn::apps::check_and_mutate_request.
    //
    // `resp` may be dsn::apps::check_and_set_response, dsn::apps::check_and_mutate_response
    // and dsn::apps::update_request.
    template <typename TRequest, typename TResponse>
    static inline void make_check_value(const TRequest &req,
                                        bool value_exist,
                                        const dsn::blob &check_value,
                                        TResponse &resp)
    {
        // Must use __set_*() functions since in dsn::apps::update_request the fields are
        // declared as optional.
        resp.__set_check_value_returned(req.return_check_value);
        resp.__set_check_value_exist(req.return_check_value && value_exist);
        if (resp.check_value_exist) {
            resp.__set_check_value(check_value);
        }
    }

    // Copy check_value's fields from `update` (i.e. the single-put request) to `resp`.
    //
    // `resp` may be dsn::apps::check_and_set_response and dsn::apps::check_and_mutate_response.
    template <typename TResponse>
    static inline void copy_check_value(const dsn::apps::update_request &update, TResponse &resp)
    {
        resp.check_value_returned = update.check_value_returned;
        resp.check_value_exist = update.check_value_exist;
        resp.check_value = update.check_value;
    }

    // Build `update` (i.e. the corresponding single-put request for a check_and_set request)
    // based on `key`, `value` and `expire_ts_seconds`. Return current status for RocksDB
    // (i.e. kOk).
    static inline int make_idempotent_request_for_check_and_set(const dsn::blob &key,
                                                                const dsn::blob &value,
                                                                int32_t expire_ts_seconds,
                                                                dsn::apps::update_request &update)
    {
        make_idempotent_request(
            key, value, expire_ts_seconds, dsn::apps::update_type::UT_CHECK_AND_SET, update);
        return rocksdb::Status::kOk;
    }

    // Build a single-put `update` for a mutate of MO_PUT in a check_and_mutate request based
    // on `key`, `value` and `expire_ts_seconds`.
    static inline void
    make_idempotent_request_for_check_and_mutate_put(const dsn::blob &key,
                                                     const dsn::blob &value,
                                                     int32_t expire_ts_seconds,
                                                     dsn::apps::update_request &update)
    {
        make_idempotent_request(
            key, value, expire_ts_seconds, dsn::apps::update_type::UT_CHECK_AND_MUTATE_PUT, update);
    }

    // Build a single-remove `update` for a mutate of MO_DELETE in a check_and_mutate request
    // based on `key`.
    static inline void
    make_idempotent_request_for_check_and_mutate_remove(const dsn::blob &key,
                                                        dsn::apps::update_request &update)
    {
        make_idempotent_request(key, dsn::apps::update_type::UT_CHECK_AND_MUTATE_REMOVE, update);
    }

    // Build response `resp` with basic info.
    template <typename TResponse>
    inline void make_basic_response(int64_t decree, TResponse &resp)
    {
        const auto pid = get_gpid();
        resp.app_id = pid.get_app_id();
        resp.partition_index = pid.get_partition_index();
        resp.decree = decree;
        resp.server = _primary_host_port;
    }

    // Build response `resp` based on `err` only for the error case (i.e. the current status
    // `err` for RocksDB is not rocksdb::Status::kOk). Return `err`.
    //
    // `resp` may be dsn::apps::incr_response,  dsn::apps::check_and_set_response and
    // dsn::apps::check_and_mutate_response.
    template <typename TResponse>
    inline int make_error_response(int err, TResponse &resp)
    {
        CHECK(err != rocksdb::Status::kOk, "this response is built only for error");
        resp.error = err;

        const auto pid = get_gpid();
        resp.app_id = pid.get_app_id();
        resp.partition_index = pid.get_partition_index();

        // Currently the mutation has not been assigned with valid decree, thus set to -1.
        resp.decree = -1;

        resp.server = _primary_host_port;

        return err;
    }

    // Build the incr response `resp` based on `err` and `new_value` only for the error case
    // (i.e. the current status `err` for RocksDB is not rocksdb::Status::kOk). Return `err`.
    inline int make_error_response(int err, int64_t new_value, dsn::apps::incr_response &resp)
    {
        resp.new_value = new_value;
        return make_error_response(err, resp);
    }

    // return true if the check type is supported
    static bool is_check_type_supported(::dsn::apps::cas_check_type::type check_type)
    {
        return check_type >= ::dsn::apps::cas_check_type::CT_NO_CHECK &&
               check_type <= ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER;
    }

    // Check whether the conditions are met for check_and_set and check_and_mutate based on
    // `check_type`, `check_operand`, `value_exist` and `check_value`.
    //
    // `decree` is not used to decide. It is only used for logging. Its type is designed to
    // be a template parameter, for the reason that this function might be called before the
    // decree is assigned for a mutation, for example, while traslating an atomic write request
    // to a single-put request. If unassigned, it could be any type as long as it is printable.
    //
    // `invalid_argument` is only used while comparing integers: if `check_operand` or
    // `check_value` is not valid integer, `invalid_argument' would be set true, and the
    // function would return false.
    //
    // Return true if check passed, otherwise false.
    template <typename TDecree>
    bool validate_check(TDecree decree,
                        ::dsn::apps::cas_check_type::type check_type,
                        const ::dsn::blob &check_operand,
                        bool value_exist,
                        const ::dsn::blob &check_value,
                        bool &invalid_argument)
    {
        invalid_argument = false;
        switch (check_type) {
        case ::dsn::apps::cas_check_type::CT_NO_CHECK:
            return true;

        case ::dsn::apps::cas_check_type::CT_VALUE_NOT_EXIST:
            return !value_exist;

        case ::dsn::apps::cas_check_type::CT_VALUE_NOT_EXIST_OR_EMPTY:
            return !value_exist || check_value.empty();

        case ::dsn::apps::cas_check_type::CT_VALUE_EXIST:
            return value_exist;

        case ::dsn::apps::cas_check_type::CT_VALUE_NOT_EMPTY:
            return value_exist && !check_value.empty();

        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE:
        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_PREFIX:
        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_POSTFIX: {
            if (!value_exist) {
                return false;
            }

            if (check_operand.empty()) {
                return true;
            }

            if (check_value.length() < check_operand.length()) {
                return false;
            }

            if (check_type == ::dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE) {
                return check_value.contains(check_operand);
            }

            if (check_type == ::dsn::apps::cas_check_type::CT_VALUE_MATCH_PREFIX) {
                return check_value.starts_with(check_operand);
            }

            CHECK(check_type == ::dsn::apps::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                  "check_type({}) should be CT_VALUE_MATCH_POSTFIX",
                  cas_check_type_to_string(check_type));

            return check_value.ends_with(check_operand);
        }

        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_LESS:
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_LESS_OR_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_GREATER_OR_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_GREATER: {
            if (!value_exist) {
                return false;
            }

            const int c = check_value.compare(check_operand);
            if (c < 0) {
                return check_type <= ::dsn::apps::cas_check_type::CT_VALUE_BYTES_LESS_OR_EQUAL;
            }

            if (c > 0) {
                return check_type >= ::dsn::apps::cas_check_type::CT_VALUE_BYTES_GREATER_OR_EQUAL;
            }

            // Must be c == 0.
            return check_type >= ::dsn::apps::cas_check_type::CT_VALUE_BYTES_LESS_OR_EQUAL &&
                   check_type <= ::dsn::apps::cas_check_type::CT_VALUE_BYTES_GREATER_OR_EQUAL;
        }

        case ::dsn::apps::cas_check_type::CT_VALUE_INT_LESS:
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_LESS_OR_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER_OR_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER: {
            if (!value_exist) {
                return false;
            }

            int64_t check_value_int = 0;
            if (!dsn::buf2int64(check_value.to_string_view(), check_value_int)) {
                // `check_value` is not a valid int64.
                LOG_ERROR_PREFIX("check failed: decree = {}, error = check value \"{}\" is "
                                 "not a valid integer or out of range",
                                 decree,
                                 utils::c_escape_sensitive_string(check_value));
                invalid_argument = true;
                return false;
            }

            int64_t check_operand_int = 0;
            if (!dsn::buf2int64(check_operand.to_string_view(), check_operand_int)) {
                // `check_operand` is not a valid int64.
                LOG_ERROR_PREFIX("check failed: decree = {}, error = check operand \"{}\" is "
                                 "not a valid integer or out of range",
                                 decree,
                                 utils::c_escape_sensitive_string(check_operand));
                invalid_argument = true;
                return false;
            }

            if (check_value_int < check_operand_int) {
                return check_type <= ::dsn::apps::cas_check_type::CT_VALUE_INT_LESS_OR_EQUAL;
            }

            if (check_value_int > check_operand_int) {
                return check_type >= ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER_OR_EQUAL;
            }

            // Must be check_value_int == check_operand_int.
            return check_type >= ::dsn::apps::cas_check_type::CT_VALUE_INT_LESS_OR_EQUAL &&
                   check_type <= ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER_OR_EQUAL;
        }

        default:
            CHECK(false, "unsupported check type: {}", check_type);
        }

        return false;
    }

    // The same as the above function, only used when the decree has not been assigned.
    bool validate_check(::dsn::apps::cas_check_type::type check_type,
                        const ::dsn::blob &check_operand,
                        bool value_exist,
                        const ::dsn::blob &check_value,
                        bool &invalid_argument)
    {
        return validate_check(
            "UNASSIGNED", check_type, check_operand, value_exist, check_value, invalid_argument);
    }

    friend class pegasus_write_service_test;
    friend class pegasus_server_write_test;
    friend class PegasusWriteServiceImplTest;
    friend class rocksdb_wrapper_test;

    const std::string _primary_host_port;
    const uint32_t _pegasus_data_version;

    std::unique_ptr<rocksdb_wrapper> _rocksdb_wrapper;

    // for setting update_response.error after committed.
    std::vector<dsn::apps::update_response *> _update_responses;
};

} // namespace pegasus::server
