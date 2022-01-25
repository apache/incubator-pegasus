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

#include "pegasus_write_service.h"
#include "pegasus_server_impl.h"
#include "logging_utils.h"

#include "base/pegasus_key_schema.h"
#include "meta_store.h"
#include "rocksdb_wrapper.h"

#include <dsn/utility/filesystem.h>
#include <dsn/utility/string_conv.h>
#include <gtest/gtest_prod.h>
#include <dsn/utility/defer.h>

namespace pegasus {
namespace server {

/// internal error codes used for fail injection
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

inline int get_cluster_id_if_exists()
{
    // cluster_id is 0 if not configured, which means it will accept writes
    // from any cluster as long as the timestamp is larger.
    static auto cluster_id_res =
        dsn::replication::get_duplication_cluster_id(dsn::get_current_cluster_name());
    static uint64_t cluster_id = cluster_id_res.is_ok() ? cluster_id_res.get_value() : 0;
    return cluster_id;
}

inline dsn::error_code get_external_files_path(const std::string &bulk_load_dir,
                                               const bool verify_before_ingest,
                                               const dsn::replication::bulk_load_metadata &metadata,
                                               /*out*/ std::vector<std::string> &files_path)
{
    for (const auto &f_meta : metadata.files) {
        const auto &file_name = dsn::utils::filesystem::path_combine(bulk_load_dir, f_meta.name);
        if (verify_before_ingest &&
            !dsn::utils::filesystem::verify_file(file_name, f_meta.md5, f_meta.size)) {
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
          _primary_address(server->_primary_address),
          _pegasus_data_version(server->_pegasus_data_version),
          _pfc_recent_expire_count(server->_pfc_recent_expire_count)
    {
        _rocksdb_wrapper = dsn::make_unique<rocksdb_wrapper>(server);
    }

    int empty_put(int64_t decree)
    {
        int err =
            _rocksdb_wrapper->write_batch_put(decree, dsn::string_view(), dsn::string_view(), 0);
        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        if (err) {
            return err;
        }

        err = _rocksdb_wrapper->write(decree);
        return err;
    }

    int multi_put(const db_write_context &ctx,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp)
    {
        int64_t decree = ctx.decree;
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (update.kvs.empty()) {
            derror_replica("invalid argument for multi_put: decree = {}, error = {}",
                           decree,
                           "request.kvs is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        for (auto &kv : update.kvs) {
            resp.error = _rocksdb_wrapper->write_batch_put_ctx(
                ctx,
                composite_raw_key(update.hash_key, kv.key),
                kv.value,
                static_cast<uint32_t>(update.expire_ts_seconds));
            if (resp.error) {
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
        resp.server = _primary_address;

        if (update.sort_keys.empty()) {
            derror_replica("invalid argument for multi_remove: decree = {}, error = {}",
                           decree,
                           "request.sort_keys is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        auto cleanup = dsn::defer([this]() { _rocksdb_wrapper->clear_up_write_batch(); });
        for (auto &sort_key : update.sort_keys) {
            resp.error = _rocksdb_wrapper->write_batch_delete(
                decree, composite_raw_key(update.hash_key, sort_key));
            if (resp.error) {
                return resp.error;
            }
        }

        resp.error = _rocksdb_wrapper->write(decree);
        if (resp.error == 0) {
            resp.count = update.sort_keys.size();
        }
        return resp.error;
    }

    int incr(int64_t decree, const dsn::apps::incr_request &update, dsn::apps::incr_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        dsn::string_view raw_key(update.key.data(), update.key.length());
        int64_t new_value = 0;
        uint32_t new_expire_ts = 0;
        db_get_context get_ctx;
        int err = _rocksdb_wrapper->get(raw_key, &get_ctx);
        if (err != 0) {
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
                if (!dsn::buf2int64(old_value, old_value_int)) {
                    // invalid old value
                    derror_replica("incr failed: decree = {}, error = "
                                   "old value \"{}\" is not an integer or out of range",
                                   decree,
                                   utils::c_escape_string(old_value));
                    resp.error = rocksdb::Status::kInvalidArgument;
                    // we should write empty record to update rocksdb's last flushed decree
                    return empty_put(decree);
                }
                new_value = old_value_int + update.increment;
                if ((update.increment > 0 && new_value < old_value_int) ||
                    (update.increment < 0 && new_value > old_value_int)) {
                    // new value is out of range, return old value by 'new_value'
                    derror_replica("incr failed: decree = {}, error = "
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
            decree, update.key, std::to_string(new_value), new_expire_ts);
        if (resp.error) {
            return resp.error;
        }

        resp.error = _rocksdb_wrapper->write(decree);
        if (resp.error == 0) {
            resp.new_value = new_value;
        }
        return resp.error;
    }

    int check_and_set(int64_t decree,
                      const dsn::apps::check_and_set_request &update,
                      dsn::apps::check_and_set_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (!is_check_type_supported(update.check_type)) {
            derror_replica("invalid argument for check_and_set: decree = {}, error = {}",
                           decree,
                           "check type {} not supported",
                           update.check_type);
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        ::dsn::blob check_key;
        pegasus_generate_key(check_key, update.hash_key, update.check_sort_key);

        db_get_context get_context;
        dsn::string_view check_raw_key(check_key.data(), check_key.length());
        int err = _rocksdb_wrapper->get(check_raw_key, &get_context);
        if (err != 0) {
            // read check value failed
            derror_rocksdb("Error to GetCheckValue for CheckAndSet decree: {}, hash_key: {}, "
                           "check_sort_key: {}",
                           decree,
                           utils::c_escape_string(update.hash_key),
                           utils::c_escape_string(update.check_sort_key));
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
                set_key,
                update.set_value,
                static_cast<uint32_t>(update.set_expire_ts_seconds));
        } else {
            // check not passed, write empty record to update rocksdb's last flushed decree
            resp.error = _rocksdb_wrapper->write_batch_put(
                decree, dsn::string_view(), dsn::string_view(), 0);
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
            resp.error =
                invalid_argument ? rocksdb::Status::kInvalidArgument : rocksdb::Status::kTryAgain;
        }

        return 0;
    }

    int check_and_mutate(int64_t decree,
                         const dsn::apps::check_and_mutate_request &update,
                         dsn::apps::check_and_mutate_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (update.mutate_list.empty()) {
            derror_replica("invalid argument for check_and_mutate: decree = {}, error = {}",
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
                derror_replica("invalid argument for check_and_mutate: decree = {}, error = "
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
            derror_replica("invalid argument for check_and_mutate: decree = {}, error = {}",
                           decree,
                           "check type {} not supported",
                           update.check_type);
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        ::dsn::blob check_key;
        pegasus_generate_key(check_key, update.hash_key, update.check_sort_key);

        db_get_context get_context;
        dsn::string_view check_raw_key(check_key.data(), check_key.length());
        int err = _rocksdb_wrapper->get(check_raw_key, &get_context);
        if (err != 0) {
            // read check value failed
            derror_rocksdb("Error to GetCheckValue for CheckAndMutate decree: {}, hash_key: {}, "
                           "check_sort_key: {}",
                           decree,
                           utils::c_escape_string(update.hash_key),
                           utils::c_escape_string(update.check_sort_key));
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
                        decree, key, m.value, static_cast<uint32_t>(m.set_expire_ts_seconds));
                } else {
                    dassert_f(m.operation == ::dsn::apps::mutate_operation::MO_DELETE,
                              "m.operation = %d",
                              m.operation);
                    resp.error = _rocksdb_wrapper->write_batch_delete(decree, key);
                }

                // in case of failure, cancel mutations
                if (resp.error)
                    break;
            }
        } else {
            // check not passed, write empty record to update rocksdb's last flushed decree
            resp.error = _rocksdb_wrapper->write_batch_put(
                decree, dsn::string_view(), dsn::string_view(), 0);
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
            resp.error =
                invalid_argument ? rocksdb::Status::kInvalidArgument : rocksdb::Status::kTryAgain;
        }
        return 0;
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
            dwarn_replica("out-dated ingestion request, ballot changed, request({}) vs "
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
                         0)) {
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
            ctx, update.key, update.value, static_cast<uint32_t>(update.expire_ts_seconds));
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp)
    {
        resp.error = _rocksdb_wrapper->write_batch_delete(decree, key);
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
            resp.server = _primary_address;
            for (dsn::apps::update_response *uresp : _update_responses) {
                *uresp = resp;
            }
            _update_responses.clear();
        }

        _rocksdb_wrapper->clear_up_write_batch();
    }

    static dsn::blob composite_raw_key(dsn::string_view hash_key, dsn::string_view sort_key)
    {
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, hash_key, sort_key);
        return raw_key;
    }

    // return true if the check type is supported
    static bool is_check_type_supported(::dsn::apps::cas_check_type::type check_type)
    {
        return check_type >= ::dsn::apps::cas_check_type::CT_NO_CHECK &&
               check_type <= ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER;
    }

    // return true if check passed.
    // for int compare, if check operand or value are not valid integer, then return false,
    // and set out param `invalid_argument' to false.
    bool validate_check(int64_t decree,
                        ::dsn::apps::cas_check_type::type check_type,
                        const ::dsn::blob &check_operand,
                        bool value_exist,
                        const ::dsn::blob &value,
                        bool &invalid_argument)
    {
        invalid_argument = false;
        switch (check_type) {
        case ::dsn::apps::cas_check_type::CT_NO_CHECK:
            return true;
        case ::dsn::apps::cas_check_type::CT_VALUE_NOT_EXIST:
            return !value_exist;
        case ::dsn::apps::cas_check_type::CT_VALUE_NOT_EXIST_OR_EMPTY:
            return !value_exist || value.length() == 0;
        case ::dsn::apps::cas_check_type::CT_VALUE_EXIST:
            return value_exist;
        case ::dsn::apps::cas_check_type::CT_VALUE_NOT_EMPTY:
            return value_exist && value.length() != 0;
        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE:
        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_PREFIX:
        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_POSTFIX: {
            if (!value_exist)
                return false;
            if (check_operand.length() == 0)
                return true;
            if (value.length() < check_operand.length())
                return false;
            if (check_type == ::dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE) {
                return dsn::string_view(value).find(check_operand) != dsn::string_view::npos;
            } else if (check_type == ::dsn::apps::cas_check_type::CT_VALUE_MATCH_PREFIX) {
                return ::memcmp(value.data(), check_operand.data(), check_operand.length()) == 0;
            } else { // check_type == ::dsn::apps::cas_check_type::CT_VALUE_MATCH_POSTFIX
                return ::memcmp(value.data() + value.length() - check_operand.length(),
                                check_operand.data(),
                                check_operand.length()) == 0;
            }
        }
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_LESS:
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_LESS_OR_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_GREATER_OR_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_BYTES_GREATER: {
            if (!value_exist)
                return false;
            int c = dsn::string_view(value).compare(dsn::string_view(check_operand));
            if (c < 0) {
                return check_type <= ::dsn::apps::cas_check_type::CT_VALUE_BYTES_LESS_OR_EQUAL;
            } else if (c == 0) {
                return check_type >= ::dsn::apps::cas_check_type::CT_VALUE_BYTES_LESS_OR_EQUAL &&
                       check_type <= ::dsn::apps::cas_check_type::CT_VALUE_BYTES_GREATER_OR_EQUAL;
            } else { // c > 0
                return check_type >= ::dsn::apps::cas_check_type::CT_VALUE_BYTES_GREATER_OR_EQUAL;
            }
        }
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_LESS:
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_LESS_OR_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER_OR_EQUAL:
        case ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER: {
            if (!value_exist)
                return false;
            int64_t check_value_int;
            if (!dsn::buf2int64(value, check_value_int)) {
                // invalid check value
                derror_replica("check failed: decree = {}, error = "
                               "check value \"{}\" is not an integer or out of range",
                               decree,
                               utils::c_escape_string(value));
                invalid_argument = true;
                return false;
            }
            int64_t check_operand_int;
            if (!dsn::buf2int64(check_operand, check_operand_int)) {
                // invalid check operand
                derror_replica("check failed: decree = {}, error = "
                               "check operand \"{}\" is not an integer or out of range",
                               decree,
                               utils::c_escape_string(check_operand));
                invalid_argument = true;
                return false;
            }
            if (check_value_int < check_operand_int) {
                return check_type <= ::dsn::apps::cas_check_type::CT_VALUE_INT_LESS_OR_EQUAL;
            } else if (check_value_int == check_operand_int) {
                return check_type >= ::dsn::apps::cas_check_type::CT_VALUE_INT_LESS_OR_EQUAL &&
                       check_type <= ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER_OR_EQUAL;
            } else { // check_value_int > check_operand_int
                return check_type >= ::dsn::apps::cas_check_type::CT_VALUE_INT_GREATER_OR_EQUAL;
            }
        }
        default:
            dassert(false, "unsupported check type: %d", check_type);
        }
        return false;
    }

private:
    friend class pegasus_write_service_test;
    friend class pegasus_server_write_test;
    friend class pegasus_write_service_impl_test;
    friend class rocksdb_wrapper_test;
    FRIEND_TEST(pegasus_write_service_impl_test, put_verify_timetag);
    FRIEND_TEST(pegasus_write_service_impl_test, verify_timetag_compatible_with_version_0);

    const std::string _primary_address;
    const uint32_t _pegasus_data_version;

    ::dsn::perf_counter_wrapper &_pfc_recent_expire_count;

    std::unique_ptr<rocksdb_wrapper> _rocksdb_wrapper;

    // for setting update_response.error after committed.
    std::vector<dsn::apps::update_response *> _update_responses;
};

} // namespace server
} // namespace pegasus
