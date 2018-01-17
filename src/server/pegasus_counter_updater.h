// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/singleton.h>
#include <dsn/utility/synchronize.h>
#include <dsn/c/api_utilities.h>
#include <dsn/cpp/json_helper.h>
#include <dsn/cpp/perf_counter_wrapper.h>
#include <dsn/tool-api/perf_counter.h>

#include <boost/asio.hpp>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/keyvalq_struct.h>

namespace pegasus {
namespace server {

enum perf_counter_target
{
    T_DEBUG = 0,
    T_FALCON,
    T_TARGET_COUNT
};

// Falcon field description:
//   http://git.n.xiaomi.com/falcon/doc/wikis/instance_monitor
struct falcon_metric
{
    std::string endpoint;    // metric host
    std::string metric;      // metric name
    int64_t timestamp;       // report time in unix seconds
    int32_t step;            // report interval in seconds
    double value;            // metric value
    std::string counterType; // GAUGE or COUNTER
    std::string tags;        // metric description, such as cluster/service
    DEFINE_JSON_SERIALIZATION(endpoint, metric, timestamp, step, value, counterType, tags)
};

class pegasus_counter_updater : public ::dsn::utils::singleton<pegasus_counter_updater>
{
public:
    pegasus_counter_updater();
    virtual ~pegasus_counter_updater();
    void start();
    void stop();
    std::string get_brief_stat();
    std::string get_perf_counters(const std::vector<std::string> &args);

private:
    void stat_intialize();
    void falcon_initialize();

    void update_brief_stat(const std::vector<dsn::perf_counter_ptr> &counters,
                           const std::vector<double> &values);
    void logging_counters(const std::vector<dsn::perf_counter_ptr> &counters,
                          const std::vector<double> &values);
    void update_counters_to_falcon(const std::vector<dsn::perf_counter_ptr> &counters,
                                   const std::vector<double> &values,
                                   int64_t timestamp);

    void update();
    void http_post_request(const std::string &host,
                           int32_t port,
                           const std::string &path,
                           const std::string &content_type,
                           const std::string &data);
    void on_report_timer(std::shared_ptr<boost::asio::deadline_timer> timer,
                         const boost::system::error_code &ec);
    static void http_request_done(struct evhttp_request *req, void *arg);

    mutable ::dsn::utils::rw_lock_nr _lock;
    std::string _local_host;
    uint16_t _local_port;
    std::string _app_name;
    std::string _cluster_name;

    uint32_t _update_interval_seconds;
    uint64_t _last_report_time_ms;
    std::shared_ptr<boost::asio::deadline_timer> _report_timer;

    // perf counter flags
    bool _enable_stat;
    bool _enable_logging;
    bool _enable_falcon;

    // falcon related
    std::string _falcon_host;
    uint16_t _falcon_port;
    std::string _falcon_path;
    falcon_metric _falcon_metric;

    int _brief_stat_count;
    std::vector<double> _brief_stat_value;

    ::dsn::utils::ex_lock_nr _last_counter_lock; // protected the following fields
    std::vector<dsn::perf_counter_ptr> _last_metrics;
    std::vector<double> _last_values;
    int64_t _last_timestamp;

    // perf counters
    ::dsn::perf_counter_wrapper _pfc_memused_virt;
    ::dsn::perf_counter_wrapper _pfc_memused_res;

private:
    static const char *perf_counter_type(dsn_perf_counter_type_t type,
                                         perf_counter_target target_type);
};
}
} // namespace
