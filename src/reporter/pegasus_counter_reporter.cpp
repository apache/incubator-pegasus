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

#include "pegasus_counter_reporter.h"

#include <alloca.h>
#include <boost/asio.hpp> // IWYU pragma: keep
#include <boost/asio/basic_deadline_timer.hpp>
#include <boost/asio/detail/impl/epoll_reactor.hpp>
#include <boost/asio/detail/impl/timer_queue_ptime.ipp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/system/error_code.hpp>
#include <event2/buffer.h>
#include <event2/event.h>
#include <fmt/core.h>
#include <prometheus/detail/gauge_builder.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <algorithm>
#include <functional>
#include <iomanip>
#include <ios>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

#include "base/pegasus_utils.h"
#include "common/common.h"
#include "pegasus_io_service.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counters.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/service_app.h"
#include "utils/api_utilities.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

DSN_DEFINE_uint64(pegasus.server,
                  perf_counter_update_interval_seconds,
                  10,
                  "perf_counter_update_interval_seconds");
DSN_DEFINE_bool(pegasus.server, perf_counter_enable_logging, true, "perf_counter_enable_logging");

DSN_DEFINE_string(pegasus.server, perf_counter_sink, "", "perf_counter_sink");

DSN_DEFINE_uint64(pegasus.server, prometheus_port, 9091, "prometheus exposer port");

DSN_DEFINE_string(pegasus.server, falcon_host, "127.0.0.1", "falcon agent host");
DSN_DEFINE_uint64(pegasus.server, falcon_port, 1988, "falcon agent port");
DSN_DEFINE_string(pegasus.server, falcon_path, "/v1/push", "falcon agent http path");

namespace pegasus {
namespace server {

static std::string get_hostname()
{
    char hostname[1024];

    if (::gethostname(hostname, sizeof(hostname))) {
        return {};
    }
    return hostname;
}

static void format_metrics_name(std::string &metrics_name)
{
    replace(metrics_name.begin(), metrics_name.end(), '@', ':');
    replace(metrics_name.begin(), metrics_name.end(), '#', ':');
    replace(metrics_name.begin(), metrics_name.end(), '.', '_');
    replace(metrics_name.begin(), metrics_name.end(), '*', '_');
    replace(metrics_name.begin(), metrics_name.end(), '(', '_');
    replace(metrics_name.begin(), metrics_name.end(), ')', '_');
}

static void libevent_log(int severity, const char *msg)
{
    dsn_log_level_t level;
    if (severity == EVENT_LOG_DEBUG)
        level = LOG_LEVEL_DEBUG;
    else if (severity == EVENT_LOG_MSG)
        level = LOG_LEVEL_INFO;
    else if (severity == EVENT_LOG_WARN)
        level = LOG_LEVEL_WARNING;
    else
        level = LOG_LEVEL_ERROR;
    dlog(level, msg);
}

pegasus_counter_reporter::pegasus_counter_reporter()
    : _local_port(0), _last_report_time_ms(0), _perf_counter_sink(perf_counter_sink_t::INVALID)
{
}

pegasus_counter_reporter::~pegasus_counter_reporter() { stop(); }

void pegasus_counter_reporter::prometheus_initialize()
{
    _registry = std::make_shared<prometheus::Registry>();
    _exposer =
        std::make_unique<prometheus::Exposer>(fmt::format("0.0.0.0:{}", FLAGS_prometheus_port));
    _exposer->RegisterCollectable(_registry);

    LOG_INFO("prometheus exposer [0.0.0.0:{}] started", FLAGS_prometheus_port);
}

void pegasus_counter_reporter::falcon_initialize()
{
    _falcon_metric.endpoint = _local_host;
    _falcon_metric.step = FLAGS_perf_counter_update_interval_seconds;
    _falcon_metric.tags = fmt::format(
        "service=pegasus,cluster={},job={},port={}", _cluster_name, _app_name, _local_port);

    LOG_INFO(
        "falcon initialize: endpoint({}), tag({})", _falcon_metric.endpoint, _falcon_metric.tags);
}

void pegasus_counter_reporter::start()
{
    ::dsn::utils::auto_write_lock l(_lock);
    if (_report_timer != nullptr)
        return;

    dsn::rpc_address addr(dsn_primary_address());
    char buf[1000];
    pegasus::utils::addr2host(addr, buf, 1000);
    _local_host = buf;
    _local_port = addr.port();

    _app_name = dsn::service_app::current_service_app_info().full_name;

    _cluster_name = dsn::get_current_cluster_name();

    _last_report_time_ms = dsn_now_ms();

    if (dsn::utils::iequals("prometheus", FLAGS_perf_counter_sink)) {
        _perf_counter_sink = perf_counter_sink_t::PROMETHEUS;
    } else if (dsn::utils::iequals("falcon", FLAGS_perf_counter_sink)) {
        _perf_counter_sink = perf_counter_sink_t::FALCON;
    } else {
        _perf_counter_sink = perf_counter_sink_t::INVALID;
    }

    if (perf_counter_sink_t::FALCON == _perf_counter_sink) {
        falcon_initialize();
    }

    if (perf_counter_sink_t::PROMETHEUS == _perf_counter_sink) {
        prometheus_initialize();
    }

    event_set_log_callback(libevent_log);

    _report_timer.reset(new boost::asio::deadline_timer(pegasus_io_service::instance().ios));
    _report_timer->expires_from_now(
        boost::posix_time::seconds(rand() % FLAGS_perf_counter_update_interval_seconds + 1));
    _report_timer->async_wait(std::bind(
        &pegasus_counter_reporter::on_report_timer, this, _report_timer, std::placeholders::_1));
}

void pegasus_counter_reporter::stop()
{
    ::dsn::utils::auto_write_lock l(_lock);
    if (_report_timer != nullptr) {
        _report_timer->cancel();
    }
    _exposer = nullptr;
    _registry = nullptr;
}

void pegasus_counter_reporter::update_counters_to_falcon(const std::string &result,
                                                         int64_t timestamp)
{
    LOG_INFO("update counters to falcon with timestamp = {}", timestamp);
    http_post_request(FLAGS_falcon_host,
                      FLAGS_falcon_port,
                      FLAGS_falcon_path,
                      "application/x-www-form-urlencoded",
                      result);
}

void pegasus_counter_reporter::update()
{
    uint64_t now = dsn_now_ms();
    int64_t timestamp = now / 1000;

    dsn::perf_counters::instance().take_snapshot();

    if (FLAGS_perf_counter_enable_logging) {
        std::stringstream oss;
        oss << "logging perf counter(name, type, value):" << std::endl;
        oss << std::fixed << std::setprecision(2);
        dsn::perf_counters::instance().iterate_snapshot(
            [&oss](const dsn::perf_counters::counter_snapshot &cs) {
                oss << "[" << cs.name << ", " << dsn_counter_type_to_string(cs.type) << ", "
                    << cs.value << "]" << std::endl;
            });
        LOG_INFO("{}", oss.str());
    }

    if (perf_counter_sink_t::FALCON == _perf_counter_sink) {
        std::stringstream oss;
        oss << "[";

        bool first_append = true;
        _falcon_metric.timestamp = timestamp;

        dsn::perf_counters::instance().iterate_snapshot(
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

    if (perf_counter_sink_t::PROMETHEUS == _perf_counter_sink) {
        const std::string hostname = get_hostname();
        dsn::perf_counters::instance().iterate_snapshot([&hostname, this](
            const dsn::perf_counters::counter_snapshot &cs) {
            std::string metrics_name = cs.name;

            // Splits metric_name like:
            //   "collector*app.pegasus*app_stat_multi_put_qps@1.0.p999"
            //   "collector*app.pegasus*app_stat_multi_put_qps@1.0"
            // app[0] = "1" which is the app(app name or app id)
            // app[1] = "0" which is the partition_index
            // app[2] = "p999" or "" which represent the percent
            std::string app[3] = {"", "", ""};
            std::list<std::string> lv;
            ::dsn::utils::split_args(metrics_name.c_str(), lv, '@');
            if (lv.size() > 1) {
                std::list<std::string> lv1;
                ::dsn::utils::split_args(lv.back().c_str(), lv1, '.');
                CHECK_LE(lv1.size(), 3);
                int i = 0;
                for (auto &v : lv1) {
                    app[i] = v;
                    i++;
                }
            }
            /**
             * deal with corner case, for example:
             *  replica*eon.replica*table.level.RPC_RRDB_RRDB_GET.latency(ns)@${table_name}.p999
             * in this case, app[0] = app name, app[1] = p999, app[2] = ""
             **/
            if ("p999" == app[1]) {
                app[2] = app[1];
                app[1].clear();
            }

            // create metrics that prometheus support to report data
            metrics_name = lv.front() + app[2];

            // prometheus metric_name doesn't support characters like .*()@, it only supports ":"
            // and "_" so change the name to make it all right.
            format_metrics_name(metrics_name);

            std::map<std::string, prometheus::Family<prometheus::Gauge> *>::iterator it =
                _gauge_family_map.find(metrics_name);
            if (it == _gauge_family_map.end()) {
                auto &add_gauge_family = prometheus::BuildGauge()
                                             .Name(metrics_name)
                                             .Labels({{"service", "pegasus"},
                                                      {"host_name", hostname},
                                                      {"cluster", _cluster_name},
                                                      {"pegasus_job", _app_name},
                                                      {"port", std::to_string(_local_port)}})
                                             .Register(*_registry);
                it = _gauge_family_map
                         .insert(std::pair<std::string, prometheus::Family<prometheus::Gauge> *>(
                             metrics_name, &add_gauge_family))
                         .first;
            }

            auto &second_gauge = it->second->Add({{"app", app[0]}, {"partition", app[1]}});
            second_gauge.Set(cs.value);
        });
    }

    LOG_INFO("update now_ms({}), last_report_time_ms({})", now, _last_report_time_ms);
    _last_report_time_ms = now;
}

void pegasus_counter_reporter::http_post_request(const std::string &host,
                                                 int32_t port,
                                                 const std::string &path,
                                                 const std::string &contentType,
                                                 const std::string &data)
{
    LOG_DEBUG("start update_request: {}", data);
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
        LOG_ERROR("http post request failed: unknown reason");
    } else if (req->response_code == 0) {
        LOG_ERROR("http post request failed: connection refused");
    } else if (req->response_code == HTTP_OK) {
        LOG_DEBUG("http post request succeed");
    } else {
        struct evbuffer *buf = evhttp_request_get_input_buffer(req);
        size_t len = evbuffer_get_length(buf);
        char *tmp = (char *)alloca(len + 1);
        memcpy(tmp, evbuffer_pullup(buf, -1), len);
        tmp[len] = '\0';
        LOG_ERROR("http post request failed: code = {}, code_line = {}, input_buffer = {}",
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
        timer->expires_from_now(
            boost::posix_time::seconds(FLAGS_perf_counter_update_interval_seconds));
        timer->async_wait(std::bind(
            &pegasus_counter_reporter::on_report_timer, this, timer, std::placeholders::_1));
    } else if (boost::system::errc::operation_canceled != ec) {
        CHECK(false, "pegasus report timer error!!!");
    }
}
} // namespace server
} // namespace pegasus
