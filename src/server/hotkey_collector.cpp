// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "hotkey_collector.h"

#include "base/pegasus_key_schema.h"
#include "base/pegasus_rpc_types.h"
#include <math.h>
#include <boost/functional/hash.hpp>
#include <dsn/utility/smart_pointers.h>

namespace pegasus {
namespace server {

DSN_DEFINE_int32("pegasus.server",
                 hotkey_collector_max_work_time,
                 150,
                 "the max time allowed to capture hotkey, will stop if hotkey's not found");

DSN_DEFINE_int32("pegasus.server",
                 data_capture_hash_bucket_num,
                 37,
                 "the number of data capture hash buckets");

static inline const char *hotkey_type_to_string(dsn::apps::hotkey_type::type type)
{
    return type == dsn::apps::hotkey_type::READ ? "READ" : "WRITE";
}

bool hotkey_collector::handle_operation(dsn::apps::hotkey_collector_operation::type op,
                                        std::string &err_hint)
{
    if (op == dsn::apps::hotkey_collector_operation::START) {
        return start(err_hint);
    }
    stop();
    return true;
}

/*static*/ int hotkey_collector::get_bucket_id(dsn::string_view data)
{
    size_t hash_value = boost::hash_range(data.begin(), data.end());
    return static_cast<int>(hash_value % FLAGS_data_capture_hash_bucket_num);
}

hotkey_collector::hotkey_collector(dsn::apps::hotkey_type::type hotkey_type,
                                   dsn::replication::replica_base *r_base)
    : replica_base(r_base),
      _state(collector_state::STOP),
      _coarse_result(-1),
      _hotkey_type(hotkey_type)
{
    _collector_start_time = dsn_now_s();
}

bool hotkey_collector::start(std::string &err_hint)
{
    switch (_state.load()) {
    case collector_state::COARSE:
    case collector_state::FINE:
        err_hint = fmt::format("still detecting {} hotkey, state is {}",
                               hotkey_type_to_string(_hotkey_type),
                               get_status());
        return false;
    case collector_state::FINISH:
        err_hint = fmt::format(
            "{} hotkey result has been found, you can send a stop rpc to restart hotkey detection",
            hotkey_type_to_string(_hotkey_type));
        return false;
    case collector_state::STOP:
        _collector_start_time = dsn_now_s();
        _coarse_data_collector = dsn::make_unique<hotkey_coarse_data_collector>(this);
        _state.store(collector_state::COARSE);
        ddebug_replica("starting to detect {} hotkey", hotkey_type_to_string(_hotkey_type));
        return true;
    default:
        err_hint = "invalid collector state";
        return false;
    }
}

void hotkey_collector::stop()
{
    _state.store(collector_state::STOP);
    _coarse_data_collector.reset();
    _fine_data_collector.reset();
    derror_replica("{} hotkey stopped, cache cleared", hotkey_type_to_string(_hotkey_type));
}

std::string hotkey_collector::get_status()
{
    switch (_state.load()) {
    case collector_state::COARSE:
        return "COARSE";
    case collector_state::FINE:
        return "FINE";
    case collector_state::FINISH:
        return "FINISH";
    case collector_state::STOP:
        return "STOP";
    default:
        return "invalid status";
    }
}

bool hotkey_collector::get_result(std::string &result) const
{
    if (_state.load() != collector_state::FINISH) {
        return false;
    }
    result = _fine_result;
    return true;
}

void hotkey_collector::capture_msg_data(dsn::message_ex **requests, const int count)
{
    if (is_ready_to_detect() || count == 0) {
        return;
    }
    for (int i = 0; i < count; i++) {
        dsn::task_code rpc_code(requests[i]->rpc_code());
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
            dsn::apps::multi_put_request thrift_request;
            unmarshall(requests[i], thrift_request);
            requests[i]->restore_read();
            capture_hash_key(thrift_request.hash_key, thrift_request.kvs.size());
            continue;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_INCR) {
            dsn::apps::incr_request thrift_request;
            unmarshall(requests[i], thrift_request);
            requests[i]->restore_read();
            capture_blob_data(thrift_request.key);
            continue;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
            dsn::apps::check_and_set_request thrift_request;
            unmarshall(requests[i], thrift_request);
            requests[i]->restore_read();
            capture_hash_key(thrift_request.hash_key);
            continue;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE) {
            dsn::apps::check_and_mutate_request thrift_request;
            unmarshall(requests[i], thrift_request);
            requests[i]->restore_read();
            capture_hash_key(thrift_request.hash_key);
            continue;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
            dsn::apps::update_request thrift_request;
            unmarshall(requests[i], thrift_request);
            requests[i]->restore_read();
            capture_blob_data(thrift_request.key);
            continue;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
            dsn::blob key;
            unmarshall(requests[i], key);
            requests[i]->restore_read();
            capture_blob_data(key);
            continue;
        }
    }
}

void hotkey_collector::capture_multi_get_data(const ::dsn::apps::multi_get_request &request)
{
    if (is_ready_to_detect()) {
        return;
    }
    capture_hash_key(request.hash_key, !request.sort_keys.empty() ? request.sort_keys.size() : 1);
}

void hotkey_collector::capture_blob_data(const ::dsn::blob &raw_key)
{
    if (is_ready_to_detect()) {
        return;
    }
    dsn::blob hash_key, sort_key;
    pegasus_restore_key(raw_key, hash_key, sort_key);
    capture_hash_key(hash_key);
}

void hotkey_collector::capture_hash_key(const dsn::blob &hash_key, int row_cnt)
{
    if (_state.load() == collector_state::COARSE && _coarse_data_collector != nullptr) {
        _coarse_data_collector->capture_data(hash_key, row_cnt);
    }
    if (_state.load() == collector_state::FINE && _fine_data_collector != nullptr) {
        _fine_data_collector->capture_data(hash_key, row_cnt);
    }
}

void hotkey_collector::analyse_data()
{
    if (is_ready_to_detect()) {
        return;
    }

    if (dsn_now_s() - _collector_start_time >= FLAGS_hotkey_collector_max_work_time) {
        derror_replica("Hotkey collector work time is exhausted but no hotkey has been found");
        stop();
        return;
    }

    if (_state.load() == collector_state::COARSE && _coarse_data_collector != nullptr) {
        _coarse_result = _coarse_data_collector->analyse_data();
        if (_coarse_result != -1) {
            _fine_data_collector =
                dsn::make_unique<hotkey_fine_data_collector>(this, _coarse_result, _hotkey_type);
            _state.store(collector_state::FINE);
            _coarse_data_collector.reset();
        }
    } else if (_state.load() == collector_state::FINE && _fine_data_collector != nullptr &&
               _fine_data_collector->analyse_data(_fine_result)) {
        derror_replica("{} hotkey result: {}",
                       hotkey_type_to_string(_hotkey_type),
                       ::pegasus::utils::c_escape_string(_fine_result));
        _state.store(collector_state::FINISH);
        _fine_data_collector.reset();
    }
}

/*static*/ int hotkey_collector::variance_calc(const std::vector<int> &data_samples, int threshold)
{
    int data_size = data_samples.size();
    double total = 0;
    int hot_index = 0;
    int hot_value = 0;
    for (int i = 0; i < data_size; i++) {
        total += data_samples[i];
        if (data_samples[i] > hot_value) {
            hot_index = i;
            hot_value = data_samples[i];
        }
    }
    // in case of sample size too small
    if (data_size < 3 || total < data_size) {
        derror("Data samples too small");
        return -1;
    }
    double avg = (total - data_samples[hot_index]) / (data_size - 1);
    double sd = 0;
    for (int j = 0; j < data_size; j++) {
        if (j != hot_index) {
            sd += pow((data_samples[j] - avg), 2);
        }
    }
    sd = sqrt(sd / (data_size - 2));
    double hot_point = (hot_value - avg) / sd;
    return hot_point > threshold ? hot_index : -1;
}

} // namespace server
} // namespace pegasus
