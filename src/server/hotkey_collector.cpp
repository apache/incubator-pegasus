// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "hotkey_collector.h"

#include "base/pegasus_key_schema.h"
#include "base/pegasus_rpc_types.h"
#include <math.h>

namespace pegasus {
namespace server {

DSN_DEFINE_int32("pegasus.server",
                 hotkey_collector_max_work_time,
                 150,
                 "after hotkey_collector_max_work_time the collection will stop automatically");

DSN_DEFINE_int32("pegasus.server",
                 data_capture_hash_bucket_num,
                 37,
                 "the number of data capture hash buckets");

bool hotkey_collector::handle_operation(dsn::apps::hotkey_collector_operation::type op,
                                        std::string &err_hint)
{
    if (op == dsn::apps::hotkey_collector_operation::START) {
        return start(err_hint);
    }
    stop();
    return true;
}

inline bool hotkey_collector::is_ready_to_detect()
{
    return (_state.load() == collector_state::STOP || _state.load() == collector_state::FINISH);
}

/*static*/ int hotkey_collector::get_bucket_id(const std::string &data)
{
    return static_cast<int>(std::hash<std::string>{}(data) % FLAGS_data_capture_hash_bucket_num);
};

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
        err_hint = fmt::format("Now is detecting {} hotkey, state is {}",
                               get_hotkey_type() == dsn::apps::hotkey_type::READ ? "read" : "write",
                               get_status());
        return false;
    case collector_state::FINISH:
        err_hint = fmt::format(
            "{} hotkey result has been found, you can send a stop rpc to restart hotkey detection",
            get_hotkey_type() == dsn::apps::hotkey_type::READ ? "Read" : "Write");
        return false;
    case collector_state::STOP:
        _collector_start_time = dsn_now_s();
        _coarse_data_collector.reset(new hotkey_coarse_data_collector(this));
        _state.store(collector_state::COARSE);
        derror_replica("Is starting to detect {} hotkey",
                       get_hotkey_type() == dsn::apps::hotkey_type::READ ? "read" : "write");
        return true;
    default:
        err_hint = "Wrong collector state";
        return false;
    }
}

void hotkey_collector::stop()
{
    _state.store(collector_state::STOP);
    _coarse_data_collector.reset();
    _fine_data_collector.reset();
    derror_replica("Already cleared {} hotkey cache",
                   get_hotkey_type() == dsn::apps::hotkey_type::READ ? "read" : "write");
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
        derror_replica("Wrong collector state");
        return "false";
    }
}

bool hotkey_collector::get_result(std::string &result) const
{
    if (_state.load() != collector_state::FINISH)
        return false;
    result = _fine_result;
    return true;
}

void hotkey_collector::capture_msg_data(dsn::message_ex **requests, const int count)
{
    if (is_ready_to_detect() || count == 0) {
        return;
    }
    for (int i = 0; i < count; i++) {
        ::dsn::blob key;
        dsn::task_code rpc_code(requests[0]->rpc_code());
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
            dsn::apps::multi_put_request thrift_request;
            unmarshall(requests[0], thrift_request);
            requests[0]->restore_read();
            key = thrift_request.hash_key;
            capture_blob_data(key, thrift_request.kvs.size());
            return;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_INCR) {
            dsn::apps::incr_request thrift_request;
            unmarshall(requests[0], thrift_request);
            requests[0]->restore_read();
            key = thrift_request.key;
            capture_blob_data(key);
            return;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
            dsn::apps::check_and_set_request thrift_request;
            unmarshall(requests[0], thrift_request);
            requests[0]->restore_read();
            key = thrift_request.hash_key;
            capture_blob_data(key);
            return;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE) {
            dsn::apps::check_and_mutate_request thrift_request;
            unmarshall(requests[0], thrift_request);
            requests[0]->restore_read();
            key = thrift_request.hash_key;
            capture_blob_data(key);
            return;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
            dsn::apps::update_request thrift_request;
            unmarshall(requests[i], thrift_request);
            requests[i]->restore_read();
            key = thrift_request.key;
            capture_blob_data(key);
            return;
        }
    }
}

void hotkey_collector::capture_multi_get_data(const ::dsn::apps::multi_get_request &request,
                                              const ::dsn::apps::multi_get_response &resp)
{
    if (is_ready_to_detect()) {
        return;
    }
    if (!resp.kvs.empty()) {
        capture_blob_data(request.hash_key, resp.kvs.size());
    } else {
        capture_blob_data(request.hash_key);
    }
}

void hotkey_collector::capture_blob_data(const ::dsn::blob &key, int count)
{
    if (is_ready_to_detect()) {
        return;
    }
    std::string hash_key, sort_key;
    pegasus_restore_key(key, hash_key, sort_key);
    capture_str_data(hash_key, count);
}

void hotkey_collector::capture_str_data(const std::string &data, int count)
{
    if (is_ready_to_detect() || data.length() == 0) {
        return;
    }
    if (_state.load() == collector_state::COARSE && _coarse_data_collector != nullptr) {
        _coarse_data_collector->capture_data(data, count);
    }
    if (_state.load() == collector_state::FINE && _fine_data_collector != nullptr) {
        _fine_data_collector->capture_data(data, count);
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
            _fine_data_collector.reset(new hotkey_fine_data_collector(this));
            _state.store(collector_state::FINE);
            _coarse_data_collector.reset();
        }
    } else if (_state.load() == collector_state::FINE && _fine_data_collector != nullptr &&
               _fine_data_collector->analyse_data(_fine_result)) {
        derror_replica("{} hotkey result: {}",
                       get_hotkey_type() == dsn::apps::hotkey_type::READ ? "read" : "write",
                       ::pegasus::utils::c_escape_string(_fine_result).c_str());
        _state.store(collector_state::FINISH);
        _fine_data_collector.reset();
    }
}

bool hotkey_collector::variance_calc(const std::vector<int> &data_samples,
                                     std::vector<int> &hot_values,
                                     int threshold)
{
    bool is_hotkey = false;
    int data_size = data_samples.size();
    double total = 0;
    for (const auto &data_sample : data_samples) {
        total += data_sample;
    }
    // in case of sample size too small
    if (data_size < 3 || total < data_size) {
        for (int i = 0; i < data_size; i++)
            hot_values.emplace_back(0);
        return false;
    }
    std::vector<double> avgs;
    std::vector<double> sds;
    for (int i = 0; i < data_size; i++) {
        double avg = (total - data_samples[i]) / (data_size - 1);
        double sd = 0;
        for (int j = 0; j < data_size; j++) {
            if (j != i) {
                sd += pow((data_samples[j] - avg), 2);
            }
        }
        sd = sqrt(sd / (data_size - 2));
        avgs.emplace_back(avg);
        sds.emplace_back(sd);
    }
    for (int i = 0; i < data_size; i++) {
        double hot_point = (data_samples[i] - avgs[i]) / sds[i];
        hot_point = ceil(std::max(hot_point, double(0)));
        hot_values.emplace_back(hot_point);
        if (hot_point >= threshold) {
            is_hotkey = true;
        }
    }
    return is_hotkey;
}

} // namespace server
} // namespace pegasus
