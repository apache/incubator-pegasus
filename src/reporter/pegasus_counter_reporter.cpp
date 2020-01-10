// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_counter_reporter.h"

#include <regex>
#include <ios>
#include <iomanip>
#include <iostream>
#include <unistd.h>
#include <dsn/cpp/service_app.h>

#include "base/pegasus_utils.h"
#include "pegasus_io_service.h"

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <fmt/format.h>

using namespace ::dsn;

static void format_metrics_name(std::string &metrics_name)
{
    replace(metrics_name.begin(), metrics_name.end(), '@', ':');
    replace(metrics_name.begin(), metrics_name.end(), '#', ':');
    replace(metrics_name.begin(), metrics_name.end(), '.', '_');
    replace(metrics_name.begin(), metrics_name.end(), '*', '_');
    replace(metrics_name.begin(), metrics_name.end(), '(', '_');
    replace(metrics_name.begin(), metrics_name.end(), ')', '_');
}

namespace pegasus {
namespace server {

static void libevent_log(int severity, const char *msg)
{
    dsn_log_level_t level;
    if (severity == EVENT_LOG_DEBUG)
        level = LOG_LEVEL_INFORMATION;
    else if (severity == EVENT_LOG_MSG)
        level = LOG_LEVEL_DEBUG;
    else if (severity == EVENT_LOG_WARN)
        level = LOG_LEVEL_WARNING;
    else
        level = LOG_LEVEL_ERROR;
    dlog(level, msg);
}

pegasus_counter_reporter::pegasus_counter_reporter()
    : _local_port(0),
      _update_interval_seconds(0),
      _last_report_time_ms(0),
      _enable_logging(false),
      _enable_falcon(false),
      _enable_prometheus(false),
      _falcon_port(0),
      _prometheus_port(0)
{
}

pegasus_counter_reporter::~pegasus_counter_reporter() { stop(); }

void pegasus_counter_reporter::prometheus_initialize()
{
    _prometheus_host = dsn_config_get_value_string(
        "pegasus.server", "prometheus_host", "127.0.0.1", "prometheus gateway host");
    _prometheus_port = (uint16_t)dsn_config_get_value_uint64(
        "pegasus.server", "prometheus_port", 9091, "prometheus gateway port");
    ddebug("prometheus initialize: host:port(%s:%d)", _prometheus_host.c_str(), _prometheus_port);

    _registry = std::make_shared<prometheus::Registry>();
    _exposer = std::make_unique<prometheus::Exposer>(
        fmt::format("{}:{}", _prometheus_host, _prometheus_port));
    _exposer->RegisterCollectable(_registry);
}

void pegasus_counter_reporter::falcon_initialize()
{
    _falcon_host = dsn_config_get_value_string(
        "pegasus.server", "falcon_host", "127.0.0.1", "falcon agent host");
    _falcon_port = (uint16_t)dsn_config_get_value_uint64(
        "pegasus.server", "falcon_port", 1988, "falcon agent port");
    _falcon_path = dsn_config_get_value_string(
        "pegasus.server", "falcon_path", "/v1/push", "falcon http path");

    _falcon_metric.endpoint = _local_host;
    _falcon_metric.step = _update_interval_seconds;

    std::stringstream tag_stream;
    tag_stream << "service"
               << "="
               << "pegasus";
    tag_stream << ","
               << "cluster"
               << "=" << _cluster_name;
    tag_stream << ","
               << "job"
               << "=" << _app_name;
    tag_stream << ","
               << "port"
               << "=" << _local_port;
    _falcon_metric.tags = tag_stream.str();

    ddebug("falcon initialize: ep(%s), tag(%s)",
           _falcon_metric.endpoint.c_str(),
           _falcon_metric.tags.c_str());
}

void pegasus_counter_reporter::start()
{
    ::dsn::utils::auto_write_lock l(_lock);
    if (_report_timer != nullptr)
        return;

    rpc_address addr(dsn_primary_address());
    char buf[1000];
    pegasus::utils::addr2host(addr, buf, 1000);
    _local_host = buf;
    _local_port = addr.port();

    _app_name = dsn::service_app::current_service_app_info().full_name;

    _cluster_name = dsn_config_get_value_string(
        "pegasus.server", "perf_counter_cluster_name", "", "perf_counter_cluster_name");
    dassert(_cluster_name.size() > 0, "");

    _update_interval_seconds = dsn_config_get_value_uint64("pegasus.server",
                                                           "perf_counter_update_interval_seconds",
                                                           10,
                                                           "perf_counter_update_interval_seconds");
    dassert(_update_interval_seconds > 0,
            "perf_counter_update_interval_seconds(%u) should > 0",
            _update_interval_seconds);
    _last_report_time_ms = dsn_now_ms();

    _enable_logging = dsn_config_get_value_bool(
        "pegasus.server", "perf_counter_enable_logging", true, "perf_counter_enable_logging");
    _enable_falcon = dsn_config_get_value_bool(
        "pegasus.server", "perf_counter_enable_falcon", false, "perf_counter_enable_falcon");
    _enable_prometheus = dsn_config_get_value_bool("pegasus.server",
                                                   "perf_counter_enable_prometheus",
                                                   false,
                                                   "perf_counter_enable_prometheus");

    if (_enable_falcon) {
        falcon_initialize();
    }

    if (_enable_prometheus) {
        prometheus_initialize();
    }

    event_set_log_callback(libevent_log);

    _report_timer.reset(new boost::asio::deadline_timer(pegasus_io_service::instance().ios));
    _report_timer->expires_from_now(
        boost::posix_time::seconds(rand() % _update_interval_seconds + 1));
    _report_timer->async_wait(std::bind(
        &pegasus_counter_reporter::on_report_timer, this, _report_timer, std::placeholders::_1));
}

void pegasus_counter_reporter::stop()
{
    ::dsn::utils::auto_write_lock l(_lock);
    if (_report_timer != nullptr) {
        _report_timer->cancel();
    }
}

void pegasus_counter_reporter::update_counters_to_falcon(const std::string &result,
                                                         int64_t timestamp)
{
    ddebug("update counters to falcon with timestamp = %" PRId64, timestamp);
    http_post_request(
        _falcon_host, _falcon_port, _falcon_path, "application/x-www-form-urlencoded", result);
}

void pegasus_counter_reporter::update()
{
    uint64_t now = dsn_now_ms();
    int64_t timestamp = now / 1000;

    perf_counters::instance().take_snapshot();

    if (_enable_logging) {
        std::stringstream oss;
        oss << "logging perf counter(name, type, value):" << std::endl;
        oss << std::fixed << std::setprecision(2);
        perf_counters::instance().iterate_snapshot(
            [&oss](const dsn::perf_counters::counter_snapshot &cs) {
                oss << "[" << cs.name << ", " << dsn_counter_type_to_string(cs.type) << ", "
                    << cs.value << "]" << std::endl;
            });
        ddebug("%s", oss.str().c_str());
    }

    if (_enable_falcon) {
        std::stringstream oss;
        oss << "[";

        bool first_append = true;
        _falcon_metric.timestamp = timestamp;

        perf_counters::instance().iterate_snapshot(
            [&oss, &first_append, this](const dsn::perf_counters::counter_snapshot &cs) {
                _falcon_metric.metric = cs.name;
                _falcon_metric.value = cs.value;
                _falcon_metric.counterType = "GAUGE";

                if (!first_append)
                    oss << ",";
                _falcon_metric.encode_json_state(oss);
                first_append = false;
            });
        oss << "]";
        update_counters_to_falcon(oss.str(), timestamp);
    }

    if (_enable_prometheus) {
        perf_counters::instance().iterate_snapshot([this](
            const dsn::perf_counters::counter_snapshot &cs) {
            std::string metrics_name = cs.name;

            // prometheus metric_name don't support characters like .*()@, it only support ":"
            // and "_"
            // so change the name to make it all right
            format_metrics_name(metrics_name);

            // split metric_name like "collector_app_pegasus_app_stat_multi_put_qps:1_0_p999" or
            // "collector_app_pegasus_app_stat_multi_put_qps:1_0"
            // app[0] = "1" which is the app_id
            // app[1] = "0" which is the partition_index
            // app[2] = "p999" or "" which represent the percent
            std::string app[3] = {"", "", ""};
            std::list<std::string> lv;
            ::dsn::utils::split_args(metrics_name.c_str(), lv, ':');
            if (lv.size() > 1) {
                std::list<std::string> lv1;
                ::dsn::utils::split_args(lv.back().c_str(), lv1, '_');
                int i = 0;
                for (auto &v : lv1) {
                    app[i] = v;
                    i++;
                }
            }

            // create metrics that prometheus support to report data
            std::map<std::string, prometheus::Family<prometheus::Gauge> *>::iterator it =
                _gauge_family_map.find(metrics_name);
            if (it == _gauge_family_map.end()) {
                auto &add_gauge_family = prometheus::BuildGauge()
                                             .Name(metrics_name)
                                             .Labels({{"service", "pegasus"},
                                                      {"cluster", _cluster_name},
                                                      {"pegasus_job", _app_name},
                                                      {"port", std::to_string(_local_port)}})
                                             .Register(*_registry);
                it = _gauge_family_map
                         .insert(std::pair<std::string, prometheus::Family<prometheus::Gauge> *>(
                             metrics_name, &add_gauge_family))
                         .first;
            }

            auto &second_gauge = it->second->Add(
                {{"app", app[0]}, {"partition", app[1]}, {"percent", app[2]}});
            second_gauge.Set(cs.value);
        });
    }

    ddebug("update now_ms(%lld), last_report_time_ms(%lld)", now, _last_report_time_ms);
    _last_report_time_ms = now;
}

void pegasus_counter_reporter::http_post_request(const std::string &host,
                                                 int32_t port,
                                                 const std::string &path,
                                                 const std::string &contentType,
                                                 const std::string &data)
{
    dinfo("start update_request, %s", data.c_str());
    struct event_base *base = event_base_new();
    struct evhttp_connection *conn = evhttp_connection_base_new(base, nullptr, host.c_str(), port);
    struct evhttp_request *req =
        evhttp_request_new(pegasus_counter_reporter::http_request_done, base);

    evhttp_add_header(req->output_headers, "Host", host.c_str());
    evhttp_add_header(req->output_headers, "Content-Type", contentType.c_str());
    evbuffer_add(req->output_buffer, data.c_str(), data.size());
    evhttp_connection_set_timeout(conn, 1);
    evhttp_make_request(conn, req, EVHTTP_REQ_POST, path.c_str());

    event_base_dispatch(base);

    // clear;
    evhttp_connection_free(conn);
    event_base_free(base);
}

void pegasus_counter_reporter::http_request_done(struct evhttp_request *req, void *arg)
{
    struct event_base *event = (struct event_base *)arg;
    if (req == nullptr) {
        derror("http post request failed: unknown reason");
    } else if (req->response_code == 0) {
        derror("http post request failed: connection refused");
    } else if (req->response_code == HTTP_OK) {
        dinfo("http post request succeed");
    } else {
        struct evbuffer *buf = evhttp_request_get_input_buffer(req);
        size_t len = evbuffer_get_length(buf);
        char *tmp = (char *)alloca(len + 1);
        memcpy(tmp, evbuffer_pullup(buf, -1), len);
        tmp[len] = '\0';
        derror("http post request failed: code = %u, code_line = %s, input_buffer = %s",
               req->response_code,
               req->response_code_line,
               tmp);
    }
    event_base_loopexit(event, 0);
}

void pegasus_counter_reporter::on_report_timer(std::shared_ptr<boost::asio::deadline_timer> timer,
                                               const boost::system::error_code &ec)
{
    // NOTICE: the following code is running out of rDSN's tls_context
    if (!ec) {
        update();
        timer->expires_from_now(boost::posix_time::seconds(_update_interval_seconds));
        timer->async_wait(std::bind(
            &pegasus_counter_reporter::on_report_timer, this, timer, std::placeholders::_1));
    } else if (boost::system::errc::operation_canceled != ec) {
        dassert(false, "pegasus report timer error!!!");
    }
}
}
} // namespace
