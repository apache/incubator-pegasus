// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/singleton.h>
#include <dsn/utility/synchronize.h>
#include <dsn/c/api_utilities.h>
#include <dsn/cpp/json_helper.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <dsn/perf_counter/perf_counter.h>

#include <iomanip>
#include <boost/asio.hpp>
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

class counter_stream
{
public:
    virtual void append(const dsn::perf_counter_ptr &ptr, double val) = 0;
    virtual std::string to_string() = 0;

protected:
    std::stringstream _stream;
};

class logging_counter : public counter_stream
{
public:
    logging_counter() : counter_stream()
    {
        _stream << "logging perf counter(name, type, value):" << std::endl;
        _stream << std::fixed << std::setprecision(2);
    }
    virtual void append(const dsn::perf_counter_ptr &ptr, double val)
    {
        _stream << "[" << ptr->full_name() << ", " << dsn_counter_type_to_string(ptr->type())
                << ", " << val << "]" << std::endl;
    }
    virtual std::string to_string() override { return _stream.str(); }
};

class falcon_counter : public counter_stream
{
public:
    falcon_counter(falcon_metric &metric, int64_t timestamp) : counter_stream(), _metric(metric)
    {
        _metric.timestamp = timestamp;
        _stream << "[";
    }
    virtual void append(const dsn::perf_counter_ptr &ptr, double val)
    {
        _metric.metric = ptr->full_name();
        _metric.value = val;
        _metric.counterType = "GAUGE";
        if (!_first_append)
            _stream << ",";
        _metric.encode_json_state(_stream);
        _first_append = false;
    }
    virtual std::string to_string() override
    {
        _stream << "]";
        return _stream.str();
    }

private:
    falcon_metric &_metric;
    bool _first_append{true};
};

class pegasus_counter_updater : public ::dsn::utils::singleton<pegasus_counter_updater>
{
public:
    pegasus_counter_updater();
    virtual ~pegasus_counter_updater();
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
