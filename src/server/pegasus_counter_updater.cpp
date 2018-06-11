// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_counter_updater.h"

#include <iomanip>
#include <regex>
#include <dsn/tool-api/command_manager.h>
#include <dsn/cpp/service_app.h>

#include "base/pegasus_utils.h"
#include "base/counter_utils.h"
#include "pegasus_io_service.h"

#if defined(__linux__)
#include <unistd.h>
#include <ios>
#include <iostream>
#include <fstream>
#endif

using namespace ::dsn;

namespace pegasus {
namespace server {

// clang-format off
static const char *s_brief_stat_mapper[] = {
    "get_qps", "zion*profiler*RPC_RRDB_RRDB_GET.qps",
    "get_p99(ns)", "zion*profiler*RPC_RRDB_RRDB_GET.latency.server",
    "multi_get_qps", "zion*profiler*RPC_RRDB_RRDB_MULTI_GET.qps",
    "multi_get_p99(ns)", "zion*profiler*RPC_RRDB_RRDB_MULTI_GET.latency.server",
    "put_qps", "zion*profiler*RPC_RRDB_RRDB_PUT.qps",
    "put_p99(ns)","zion*profiler*RPC_RRDB_RRDB_PUT.latency.server",
    "multi_put_qps", "zion*profiler*RPC_RRDB_RRDB_MULTI_PUT.qps",
    "multi_put_p99(ns)", "zion*profiler*RPC_RRDB_RRDB_MULTI_PUT.latency.server",
    "serving_replica_count", "replica*eon.replica_stub*replica(Count)",
    "opening_replica_count", "replica*eon.replica_stub*opening.replica(Count)",
    "closing_replica_count", "replica*eon.replica_stub*closing.replica(Count)",
    "commit_throughput", "replica*eon.replica_stub*replicas.commit.qps",
    "learning_count", "replica*eon.replica_stub*replicas.learning.count",
    "manual_compact_running_count", "replica*app.pegasus*manual.compact.running.count",
    "manual_compact_enqueue_count", "replica*app.pegasus*manual.compact.enqueue.count",
    "shared_log_size(MB)", "replica*eon.replica_stub*shared.log.size(MB)",
    "memused_virt(MB)", "replica*server*memused.virt(MB)",
    "memused_res(MB)", "replica*server*memused.res(MB)",
    "disk_capacity_total(MB)", "replica*eon.replica_stub*disk.capacity.total(MB)",
    "disk_available_total_ratio", "replica*eon.replica_stub*disk.available.total.ratio",
    "disk_available_min_ratio", "replica*eon.replica_stub*disk.available.min.ratio",
    "disk_available_max_ratio", "replica*eon.replica_stub*disk.available.max.ratio"
};
// clang-format on

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

#if defined(__linux__)
//////////////////////////////////////////////////////////////////////////////
//
// process_mem_usage(double &, double &) - takes two doubles by reference,
// attempts to read the system-dependent data for a process' virtual memory
// size and resident set size, and return the results in KB.
//
// On failure, returns 0.0, 0.0
static void process_mem_usage(double &vm_usage, double &resident_set)
{
    using std::ios_base;
    using std::ifstream;
    using std::string;

    vm_usage = 0.0;
    resident_set = 0.0;

    // 'file' stat seems to give the most reliable results
    //
    ifstream stat_stream("/proc/self/stat", ios_base::in);

    // dummy vars for leading entries in stat that we don't care about
    //
    string pid, comm, state, ppid, pgrp, session, tty_nr;
    string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    string utime, stime, cutime, cstime, priority, nice;
    string O, itrealvalue, starttime;

    // the two fields we want
    //
    unsigned long vsize;
    long rss;

    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid >> flags >>
        minflt >> cminflt >> majflt >> cmajflt >> utime >> stime >> cutime >> cstime >> priority >>
        nice >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care about the rest

    stat_stream.close();

    static long page_size_kb =
        sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
}
#endif

pegasus_counter_updater::pegasus_counter_updater()
    : _local_port(0),
      _update_interval_seconds(0),
      _last_report_time_ms(0),
      _enable_stat(false),
      _enable_logging(false),
      _enable_falcon(false),
      _falcon_port(0),
      _brief_stat_count(0),
      _last_timestamp(0)
{
    ::dsn::command_manager::instance().register_command(
        {"server-stat"},
        "server-stat - query server statistics",
        "server-stat",
        [](const std::vector<std::string> &args) {
            return ::pegasus::server::pegasus_counter_updater::instance().get_brief_stat();
        });

    ::dsn::command_manager::instance().register_command(
        {"perf-counters"},
        "perf-counters - query perf counters, supporting filter by POSIX basic regular expressions",
        "perf-counters [name-filter]...",
        [](const std::vector<std::string> &args) {
            return ::pegasus::server::pegasus_counter_updater::instance().get_perf_counters(args);
        });
}

pegasus_counter_updater::~pegasus_counter_updater() { stop(); }

void pegasus_counter_updater::stat_intialize()
{
    _brief_stat_count = sizeof(s_brief_stat_mapper) / sizeof(char *) / 2;
    _brief_stat_value.resize(_brief_stat_count, -1);
}

void pegasus_counter_updater::falcon_initialize()
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

void pegasus_counter_updater::start()
{
    _pfc_memused_virt.init_app_counter(
        "server", "memused.virt(MB)", COUNTER_TYPE_NUMBER, "virtual memory usage in MB");
    _pfc_memused_res.init_app_counter(
        "server", "memused.res(MB)", COUNTER_TYPE_NUMBER, "physical memory usage in MB");

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

    _enable_stat = dsn_config_get_value_bool(
        "pegasus.server", "perf_counter_enable_stat", true, "perf_counter_enable_stat");
    _enable_logging = dsn_config_get_value_bool(
        "pegasus.server", "perf_counter_enable_logging", true, "perf_counter_enable_logging");
    _enable_falcon = dsn_config_get_value_bool(
        "pegasus.server", "perf_counter_enable_falcon", false, "perf_counter_enable_falcon");

    if (_enable_stat) {
        stat_intialize();
    }

    if (_enable_falcon) {
        falcon_initialize();
    }

    event_set_log_callback(libevent_log);

    _report_timer.reset(new boost::asio::deadline_timer(pegasus_io_service::instance().ios));
    _report_timer->expires_from_now(
        boost::posix_time::seconds(rand() % _update_interval_seconds + 1));
    _report_timer->async_wait(std::bind(
        &pegasus_counter_updater::on_report_timer, this, _report_timer, std::placeholders::_1));
}

void pegasus_counter_updater::stop()
{
    ::dsn::utils::auto_write_lock l(_lock);
    if (_report_timer != nullptr) {
        _report_timer->cancel();
    }
}

/*static*/
const char *pegasus_counter_updater::perf_counter_type(dsn_perf_counter_type_t t,
                                                       perf_counter_target target)
{
    static const char *type_string[] = {
        "number",
        "GAUGE", // falcon type

        "volatile_number",
        "GAUGE", // falcon type

        "rate",
        // falcon type
        // NOTICE: the "rate" is "GAUGE" coz we don't need falcon to recalculate the rate,
        // please ref the falcon manual
        "GAUGE",

        "number_percentile",
        "GAUGE", // falcon type
    };

    static const int TYPES_COUNT = sizeof(type_string) / sizeof(const char *);
    dassert(t * T_TARGET_COUNT + target < TYPES_COUNT, "type (%d)", t);
    return type_string[t * T_TARGET_COUNT + target];
}

void pegasus_counter_updater::update_counters_to_falcon(
    const std::vector<dsn::perf_counter_ptr> &counters,
    const std::vector<double> &values,
    int64_t timestamp)
{
    ddebug("update counters to falcon with timestamp = %" PRId64, timestamp);

    std::stringstream falcon_data;
    falcon_data << "[";
    for (unsigned int i = 0; i < values.size(); ++i) {
        const dsn::perf_counter_ptr &ptr = counters[i];
        _falcon_metric.metric = ptr->full_name();
        _falcon_metric.timestamp = timestamp;
        _falcon_metric.value = values[i];
        _falcon_metric.counterType = perf_counter_type(ptr->type(), T_FALCON);
        _falcon_metric.encode_json_state(falcon_data);
        if (i + 1 < values.size())
            falcon_data << ",";
    }
    falcon_data << "]";

    const std::string &result = falcon_data.str();
    http_post_request(
        _falcon_host, _falcon_port, _falcon_path, "application/x-www-form-urlencoded", result);
}

void pegasus_counter_updater::logging_counters(const std::vector<dsn::perf_counter_ptr> &counters,
                                               const std::vector<double> &values)
{
    std::stringstream logging;
    logging << "logging perf counter(name, type, value):" << std::endl;
    logging << std::fixed << std::setprecision(2);
    for (unsigned int i = 0; i < values.size(); ++i) {
        const dsn::perf_counter_ptr &ptr = counters[i];
        logging << "[" << ptr->full_name() << ", " << perf_counter_type(ptr->type(), T_DEBUG)
                << ", " << values[i] << "]" << std::endl;
    }
    ddebug(logging.str().c_str());
}

struct cmp_str
{
    bool operator()(const char *a, const char *b) { return std::strcmp(a, b) < 0; }
};
void pegasus_counter_updater::update_brief_stat(const std::vector<dsn::perf_counter_ptr> &counters,
                                                const std::vector<double> &values)
{
    std::map<const char *, int, cmp_str> pref_name_to_index;
    for (int i = 0; i < counters.size(); ++i) {
        pref_name_to_index[counters[i]->full_name()] = i;
    }
    for (int i = 0; i < _brief_stat_count; ++i) {
        const char *perf_name = s_brief_stat_mapper[i * 2 + 1];
        auto find = pref_name_to_index.find(perf_name);
        if (find != pref_name_to_index.end()) {
            _brief_stat_value[i] = values[find->second];
        }
    }
}

std::string pegasus_counter_updater::get_brief_stat()
{
    std::ostringstream oss;
    if (_enable_stat) {
        oss << std::fixed << std::setprecision(0);
        for (int i = 0; i < _brief_stat_count; ++i) {
            if (i != 0)
                oss << ", ";
            const char *stat_name = s_brief_stat_mapper[i * 2];
            oss << stat_name << "=" << _brief_stat_value[i];
        }
    } else {
        oss << "stat disabled";
    }
    return oss.str();
}

std::string pegasus_counter_updater::get_perf_counters(const std::vector<std::string> &args)
{
    std::vector<dsn::perf_counter_ptr> metrics;
    std::vector<double> values;
    int64_t timestamp = 0;
    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_last_counter_lock);
        metrics = _last_metrics;
        values = _last_values;
        timestamp = _last_timestamp;
    }

    perf_counter_info info;
    if (timestamp != 0) {
        dassert(metrics.size() == values.size(),
                "unmatched size: %d vs %d",
                (int)metrics.size(),
                (int)values.size());

        std::vector<std::regex> regs;
        regs.reserve(args.size());
        for (auto &arg : args) {
            try {
                regs.emplace_back(arg, std::regex_constants::basic);
            } catch (...) {
                info.result = "ERROR: invalid filter: " + arg;
                break;
            }
        }

        if (info.result.empty()) {
            for (int i = 0; i < metrics.size(); ++i) {
                bool matched = false;
                if (regs.empty()) {
                    matched = true;
                } else {
                    for (auto &reg : regs) {
                        if (std::regex_match(metrics[i]->full_name(), reg)) {
                            matched = true;
                            break;
                        }
                    }
                }

                if (matched) {
                    info.counters.emplace_back(
                        metrics[i]->full_name(), metrics[i]->type(), values[i]);
                }
            }

            info.result = "OK";
            info.timestamp = timestamp;
            char buf[20];
            ::dsn::utils::time_ms_to_date_time(timestamp * 1000, buf, sizeof(buf));
            info.timestamp_str = buf;
        }
    } else {
        info.result = "ERROR: counter not generated";
    }

    std::stringstream ss;
    info.encode_json_state(ss);
    return ss.str();
}

void pegasus_counter_updater::update()
{
    ddebug("start update by timer");

#if defined(__linux__)
    double vm_usage;
    double resident_set;
    process_mem_usage(vm_usage, resident_set);
    uint64_t memused_virt = (uint64_t)vm_usage / 1024;
    uint64_t memused_res = (uint64_t)resident_set / 1024;
    _pfc_memused_virt->set(memused_virt);
    _pfc_memused_res->set(memused_res);
    ddebug("memused_virt = %" PRIu64 " MB, memused_res = %" PRIu64 "MB", memused_virt, memused_res);
#endif

    std::vector<dsn::perf_counter_ptr> metrics;
    std::vector<double> values;

    ::dsn::perf_counters::instance().get_all_counters(&metrics);
    if (metrics.empty()) {
        ddebug("no need update, coz no counters added");
        return;
    }

    uint64_t now = dsn_now_ms();
    dinfo("update now_ms(%lld), last_report_time_ms(%lld)", now, _last_report_time_ms);
    _last_report_time_ms = now;

    int64_t timestamp = now / 1000;
    values.reserve(metrics.size());

    // in case the get_xxx functions for the perf counters had SIDE EFFECTS
    // we'd better pre fetch these values, and then use them
    for (auto &counter : metrics) {
        switch (counter->type()) {
        case COUNTER_TYPE_NUMBER:
        case COUNTER_TYPE_VOLATILE_NUMBER:
            values.push_back(counter->get_integer_value());
            break;
        case COUNTER_TYPE_RATE:
            values.push_back(counter->get_value());
            break;
        case COUNTER_TYPE_NUMBER_PERCENTILES:
            values.push_back(counter->get_percentile(COUNTER_PERCENTILE_99));
            break;
        default:
            dassert(false, "invalid type(%d)", counter->type());
            break;
        }
    }

    if (_enable_stat) {
        update_brief_stat(metrics, values);
    }

    if (_enable_logging) {
        logging_counters(metrics, values);
    }

    if (_enable_falcon) {
        update_counters_to_falcon(metrics, values, timestamp);
    }

    ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_last_counter_lock);
    _last_metrics = std::move(metrics);
    _last_values = std::move(values);
    _last_timestamp = timestamp;
}

void pegasus_counter_updater::http_post_request(const std::string &host,
                                                int32_t port,
                                                const std::string &path,
                                                const std::string &contentType,
                                                const std::string &data)
{
    dinfo("start update_request, %s", data.c_str());
    struct event_base *base = event_base_new();
    struct evhttp_connection *conn = evhttp_connection_base_new(base, NULL, host.c_str(), port);
    struct evhttp_request *req =
        evhttp_request_new(pegasus_counter_updater::http_request_done, base);

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

void pegasus_counter_updater::http_request_done(struct evhttp_request *req, void *arg)
{
    struct event_base *event = (struct event_base *)arg;
    if (req == nullptr) {
        derror("http_request_done failed, unknown reason");
        event_base_loopexit(event, 0);
        return;
    }

    switch (req->response_code) {
    case HTTP_OK: {
        dinfo("http_request_done OK");
        event_base_loopexit(event, 0);
    } break;

    default:
        derror("http post request receive ERROR: %u", req->response_code);

        struct evbuffer *buf = evhttp_request_get_input_buffer(req);
        size_t len = evbuffer_get_length(buf);
        char *tmp = (char *)malloc(len + 1);
        memcpy(tmp, evbuffer_pullup(buf, -1), len);
        tmp[len] = '\0';
        derror("http post request receive ERROR: %u, %s", req->response_code, tmp);
        free(tmp);
        event_base_loopexit(event, 0);
        return;
    }
}

void pegasus_counter_updater::on_report_timer(std::shared_ptr<boost::asio::deadline_timer> timer,
                                              const boost::system::error_code &ec)
{
    // NOTICE: the following code is running out of rDSN's tls_context
    if (!ec) {
        update();
        timer->expires_from_now(boost::posix_time::seconds(_update_interval_seconds));
        timer->async_wait(std::bind(
            &pegasus_counter_updater::on_report_timer, this, timer, std::placeholders::_1));
    } else if (boost::system::errc::operation_canceled != ec) {
        dassert(false, "pegasus report timer error!!!");
    }
}
}
} // namespace
