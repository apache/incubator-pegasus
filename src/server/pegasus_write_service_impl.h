// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "pegasus_write_service.h"
#include "pegasus_server_impl.h"
#include "logging_utils.h"

#include "base/pegasus_key_schema.h"

#include <dsn/utility/fail_point.h>
#include <dsn/utility/string_conv.h>

namespace pegasus {
namespace server {

/// internal error codes used for fail injection
static constexpr int FAIL_DB_WRITE_BATCH_PUT = -101;
static constexpr int FAIL_DB_WRITE_BATCH_DELETE = -102;
static constexpr int FAIL_DB_WRITE = -103;

class pegasus_write_service::impl : public dsn::replication::replica_base
{
public:
    explicit impl(pegasus_server_impl *server)
        : replica_base(*server),
          _primary_address(server->_primary_address),
          _value_schema_version(server->_value_schema_version),
          _db(server->_db),
          _wt_opts(server->_wt_opts),
          _rd_opts(server->_rd_opts),
          _default_ttl(0),
          _pfc_recent_expire_count(server->_pfc_recent_expire_count)
    {
    }

    int empty_put(int64_t decree)
    {
        int err = db_write_batch_put(decree, dsn::string_view(), dsn::string_view(), 0);
        if (err) {
            clear_up_batch_states(decree, err);
            return err;
        }

        err = db_write(decree);

        clear_up_batch_states(decree, err);
        return err;
    }

    int multi_put(int64_t decree,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp)
    {
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

        for (auto &kv : update.kvs) {
            resp.error = db_write_batch_put(decree,
                                            composite_raw_key(update.hash_key, kv.key),
                                            kv.value,
                                            static_cast<uint32_t>(update.expire_ts_seconds));
            if (resp.error) {
                clear_up_batch_states(decree, resp.error);
                return resp.error;
            }
        }

        resp.error = db_write(decree);

        clear_up_batch_states(decree, resp.error);
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

        for (auto &sort_key : update.sort_keys) {
            resp.error =
                db_write_batch_delete(decree, composite_raw_key(update.hash_key, sort_key));
            if (resp.error) {
                clear_up_batch_states(decree, resp.error);
                return resp.error;
            }
        }

        resp.error = db_write(decree);
        if (resp.error == 0) {
            resp.count = update.sort_keys.size();
        }

        clear_up_batch_states(decree, resp.error);
        return resp.error;
    }

    int incr(int64_t decree, const dsn::apps::incr_request &update, dsn::apps::incr_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        rocksdb::Slice raw_key(update.key.data(), update.key.length());
        std::string raw_value;
        int64_t new_value = 0;
        uint32_t new_expire_ts = 0;
        rocksdb::Status s = _db->Get(_rd_opts, raw_key, &raw_value);
        if (s.ok()) {
            uint32_t old_expire_ts = pegasus_extract_expire_ts(_value_schema_version, raw_value);
            if (check_if_ts_expired(utils::epoch_now(), old_expire_ts)) {
                // ttl timeout, set to 0 before increment
                _pfc_recent_expire_count->increment();
                new_value = update.increment;
                new_expire_ts = update.expire_ts_seconds > 0 ? update.expire_ts_seconds : 0;
            } else {
                ::dsn::blob old_value;
                pegasus_extract_user_data(_value_schema_version, std::move(raw_value), old_value);
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
                if (update.expire_ts_seconds == 0)
                    new_expire_ts = old_expire_ts;
                else if (update.expire_ts_seconds < 0)
                    new_expire_ts = 0;
                else // update.expire_ts_seconds > 0
                    new_expire_ts = update.expire_ts_seconds;
            }
        } else if (s.IsNotFound()) {
            // old value is not found, set to 0 before increment
            new_value = update.increment;
            new_expire_ts = update.expire_ts_seconds > 0 ? update.expire_ts_seconds : 0;
        } else {
            // read old value failed
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
            derror_rocksdb("Get for Incr",
                           s.ToString(),
                           "decree: {}, hash_key: {}, sort_key: {}",
                           decree,
                           utils::c_escape_string(hash_key),
                           utils::c_escape_string(sort_key));
            resp.error = s.code();
            return resp.error;
        }

        resp.error =
            db_write_batch_put(decree, update.key, std::to_string(new_value), new_expire_ts);
        if (resp.error) {
            clear_up_batch_states(decree, resp.error);
            return resp.error;
        }

        resp.error = db_write(decree);
        if (resp.error == 0) {
            resp.new_value = new_value;
        }

        clear_up_batch_states(decree, resp.error);
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
        rocksdb::Slice check_raw_key(check_key.data(), check_key.length());
        std::string check_raw_value;
        rocksdb::Status check_status = _db->Get(_rd_opts, check_raw_key, &check_raw_value);
        if (check_status.ok()) {
            // read check value succeed
            if (check_if_record_expired(
                    _value_schema_version, utils::epoch_now(), check_raw_value)) {
                // check value ttl timeout
                _pfc_recent_expire_count->increment();
                check_status = rocksdb::Status::NotFound();
            }
        } else if (!check_status.IsNotFound()) {
            // read check value failed
            derror_rocksdb("GetCheckValue for CheckAndSet",
                           check_status.ToString(),
                           "decree: {}, hash_key: {}, check_sort_key: {}",
                           decree,
                           utils::c_escape_string(update.hash_key),
                           utils::c_escape_string(update.check_sort_key));
            resp.error = check_status.code();
            return resp.error;
        }
        dassert_f(check_status.ok() || check_status.IsNotFound(),
                  "status = %s",
                  check_status.ToString().c_str());

        ::dsn::blob check_value;
        if (check_status.ok()) {
            pegasus_extract_user_data(
                _value_schema_version, std::move(check_raw_value), check_value);
        }

        if (update.return_check_value) {
            resp.check_value_returned = true;
            if (check_status.ok()) {
                resp.check_value_exist = true;
                resp.check_value = check_value;
            }
        }

        bool invalid_argument = false;
        bool passed = validate_check(decree,
                                     update.check_type,
                                     update.check_operand,
                                     check_status.ok(),
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
            resp.error = db_write_batch_put(decree,
                                            set_key,
                                            update.set_value,
                                            static_cast<uint32_t>(update.set_expire_ts_seconds));
        } else {
            // check not passed, write empty record to update rocksdb's last flushed decree
            resp.error = db_write_batch_put(decree, dsn::string_view(), dsn::string_view(), 0);
        }
        if (resp.error) {
            clear_up_batch_states(decree, resp.error);
            return resp.error;
        }

        resp.error = db_write(decree);
        if (resp.error) {
            clear_up_batch_states(decree, resp.error);
            return resp.error;
        }

        if (!passed) {
            // check not passed, return proper error code to user
            resp.error =
                invalid_argument ? rocksdb::Status::kInvalidArgument : rocksdb::Status::kTryAgain;
        }

        clear_up_batch_states(decree, resp.error);
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
        rocksdb::Slice check_raw_key(check_key.data(), check_key.length());
        std::string check_raw_value;
        rocksdb::Status check_status = _db->Get(_rd_opts, check_raw_key, &check_raw_value);
        if (check_status.ok()) {
            // read check value succeed
            if (check_if_record_expired(
                    _value_schema_version, utils::epoch_now(), check_raw_value)) {
                // check value ttl timeout
                _pfc_recent_expire_count->increment();
                check_status = rocksdb::Status::NotFound();
            }
        } else if (!check_status.IsNotFound()) {
            // read check value failed
            derror_rocksdb("GetCheckValue for CheckAndMutate",
                           check_status.ToString(),
                           "decree: {}, hash_key: {}, check_sort_key: {}",
                           decree,
                           utils::c_escape_string(update.hash_key),
                           utils::c_escape_string(update.check_sort_key));
            resp.error = check_status.code();
            return resp.error;
        }
        dassert_f(check_status.ok() || check_status.IsNotFound(),
                  "status = %s",
                  check_status.ToString().c_str());

        ::dsn::blob check_value;
        if (check_status.ok()) {
            pegasus_extract_user_data(
                _value_schema_version, std::move(check_raw_value), check_value);
        }

        if (update.return_check_value) {
            resp.check_value_returned = true;
            if (check_status.ok()) {
                resp.check_value_exist = true;
                resp.check_value = check_value;
            }
        }

        bool invalid_argument = false;
        bool passed = validate_check(decree,
                                     update.check_type,
                                     update.check_operand,
                                     check_status.ok(),
                                     check_value,
                                     invalid_argument);

        if (passed) {
            for (auto &m : update.mutate_list) {
                ::dsn::blob key;
                pegasus_generate_key(key, update.hash_key, m.sort_key);
                if (m.operation == ::dsn::apps::mutate_operation::MO_PUT) {
                    resp.error = db_write_batch_put(
                        decree, key, m.value, static_cast<uint32_t>(m.set_expire_ts_seconds));
                } else {
                    dassert_f(m.operation == ::dsn::apps::mutate_operation::MO_DELETE,
                              "m.operation = %d",
                              m.operation);
                    resp.error = db_write_batch_delete(decree, key);
                }

                // in case of failure, cancel mutations
                if (resp.error)
                    break;
            }
        } else {
            // check not passed, write empty record to update rocksdb's last flushed decree
            resp.error = db_write_batch_put(decree, dsn::string_view(), dsn::string_view(), 0);
        }

        if (resp.error) {
            clear_up_batch_states(decree, resp.error);
            return resp.error;
        }

        resp.error = db_write(decree);
        if (resp.error) {
            clear_up_batch_states(decree, resp.error);
            return resp.error;
        }

        if (!passed) {
            // check not passed, return proper error code to user
            resp.error =
                invalid_argument ? rocksdb::Status::kInvalidArgument : rocksdb::Status::kTryAgain;
        }

        clear_up_batch_states(decree, resp.error);
        return 0;
    }

    /// For batch write.

    int batch_put(int64_t decree,
                  const dsn::apps::update_request &update,
                  dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_put(
            decree, update.key, update.value, static_cast<uint32_t>(update.expire_ts_seconds));
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_delete(decree, key);
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_commit(int64_t decree)
    {
        int err = db_write(decree);
        clear_up_batch_states(decree, err);
        return err;
    }

    void batch_abort(int64_t decree, int err) { clear_up_batch_states(decree, err); }

    void set_default_ttl(uint32_t ttl)
    {
        if (_default_ttl != ttl) {
            _default_ttl = ttl;
            ddebug_replica("update _default_ttl to {}.", ttl);
        }
    }

private:
    int db_write_batch_put(int64_t decree,
                           dsn::string_view raw_key,
                           dsn::string_view value,
                           uint32_t expire_sec)
    {
        FAIL_POINT_INJECT_F("db_write_batch_put",
                            [](dsn::string_view) -> int { return FAIL_DB_WRITE_BATCH_PUT; });

        rocksdb::Slice skey = utils::to_rocksdb_slice(raw_key);
        rocksdb::SliceParts skey_parts(&skey, 1);
        rocksdb::SliceParts svalue =
            _value_generator.generate_value(_value_schema_version, value, db_expire_ts(expire_sec));
        rocksdb::Status s = _batch.Put(skey_parts, svalue);
        if (dsn_unlikely(!s.ok())) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
            derror_rocksdb("WriteBatchPut",
                           s.ToString(),
                           "decree: {}, hash_key: {}, sort_key: {}, expire_ts: {}",
                           decree,
                           utils::c_escape_string(hash_key),
                           utils::c_escape_string(sort_key),
                           expire_sec);
        }
        return s.code();
    }

    int db_write_batch_delete(int64_t decree, dsn::string_view raw_key)
    {
        FAIL_POINT_INJECT_F("db_write_batch_delete",
                            [](dsn::string_view) -> int { return FAIL_DB_WRITE_BATCH_DELETE; });

        rocksdb::Status s = _batch.Delete(utils::to_rocksdb_slice(raw_key));
        if (dsn_unlikely(!s.ok())) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
            derror_rocksdb("WriteBatchDelete",
                           s.ToString(),
                           "decree: {}, hash_key: {}, sort_key: {}",
                           decree,
                           utils::c_escape_string(hash_key),
                           utils::c_escape_string(sort_key));
        }
        return s.code();
    }

    // Apply the write batch into rocksdb.
    int db_write(int64_t decree)
    {
        dassert(_batch.Count() != 0, "");

        FAIL_POINT_INJECT_F("db_write", [](dsn::string_view) -> int { return FAIL_DB_WRITE; });

        _wt_opts.given_decree = static_cast<uint64_t>(decree);
        auto status = _db->Write(_wt_opts, &_batch);
        if (!status.ok()) {
            derror_rocksdb("Write", status.ToString(), "decree: {}", decree);
        }
        return status.code();
    }

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

        _batch.Clear();
    }

    dsn::blob composite_raw_key(dsn::string_view hash_key, dsn::string_view sort_key)
    {
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, hash_key, sort_key);
        return raw_key;
    }

    // return true if the check type is supported
    bool is_check_type_supported(::dsn::apps::cas_check_type::type check_type)
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

    uint32_t db_expire_ts(uint32_t expire_ts)
    {
        // use '_default_ttl' when ttl is not set for this write operation.
        if (_default_ttl != 0 && expire_ts == 0) {
            return utils::epoch_now() + _default_ttl;
        }

        return expire_ts;
    }

private:
    friend class pegasus_write_service_test;
    friend class pegasus_server_write_test;

    const std::string _primary_address;
    const uint32_t _value_schema_version;

    rocksdb::WriteBatch _batch;
    rocksdb::DB *_db;
    rocksdb::WriteOptions &_wt_opts;
    rocksdb::ReadOptions &_rd_opts;
    volatile uint32_t _default_ttl;
    ::dsn::perf_counter_wrapper &_pfc_recent_expire_count;

    pegasus_value_generator _value_generator;

    // for setting update_response.error after committed.
    std::vector<dsn::apps::update_response *> _update_responses;
};

} // namespace server
} // namespace pegasus
