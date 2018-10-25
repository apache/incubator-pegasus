// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/singleton.h>
#include <dsn/utility/synchronize.h>
#include <dsn/cpp/json_helper.h>

#include <boost/asio/deadline_timer.hpp>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/keyvalq_struct.h>

namespace pegasus {
namespace server {

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

class pegasus_counter_reporter : public ::dsn::utils::singleton<pegasus_counter_reporter>
{
public:
    pegasus_counter_reporter();
    virtual ~pegasus_counter_reporter();
    void start();
    void stop();

private:
    void falcon_initialize();

    void update_counters_to_falcon(const std::string &result, int64_t timestamp);

    void http_post_request(const std::string &host,
                           int32_t port,
                           const std::string &path,
                           const std::string &content_type,
                           const std::string &data);
    static void http_request_done(struct evhttp_request *req, void *arg);

    void update();
    void on_report_timer(std::shared_ptr<boost::asio::deadline_timer> timer,
                         const boost::system::error_code &ec);

    mutable ::dsn::utils::rw_lock_nr _lock;
    std::string _local_host;
    uint16_t _local_port;
    std::string _app_name;
    std::string _cluster_name;

    uint32_t _update_interval_seconds;
    uint64_t _last_report_time_ms;
    std::shared_ptr<boost::asio::deadline_timer> _report_timer;

    // perf counter flags
    bool _enable_logging;
    bool _enable_falcon;

    // falcon related
    std::string _falcon_host;
    uint16_t _falcon_port;
    std::string _falcon_path;
    falcon_metric _falcon_metric;
};
}
} // namespace
