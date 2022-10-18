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

#include "utils/singleton.h"
#include "utils/synchronize.h"
#include "common/json_helper.h"

#include <boost/asio/deadline_timer.hpp>
#include <event2/http.h>
#include <event2/http_struct.h>

#include <prometheus/registry.h>
#include <prometheus/exposer.h>

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

enum class perf_counter_sink_t
{
    FALCON,
    PROMETHEUS,
    INVALID
};

class pegasus_counter_reporter : public dsn::utils::singleton<pegasus_counter_reporter>
{
public:
    void start();
    void stop();

private:
    pegasus_counter_reporter();
    virtual ~pegasus_counter_reporter();

    void falcon_initialize();
    void prometheus_initialize();

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

    uint64_t _last_report_time_ms;
    std::shared_ptr<boost::asio::deadline_timer> _report_timer;

    // perf counter flags
    perf_counter_sink_t _perf_counter_sink;

    falcon_metric _falcon_metric;

    // prometheus relates
    std::shared_ptr<prometheus::Registry> _registry;
    std::unique_ptr<prometheus::Exposer> _exposer;
    std::map<std::string, prometheus::Family<prometheus::Gauge> *> _gauge_family_map;

    friend class dsn::utils::singleton<pegasus_counter_reporter>;
};
} // namespace server
} // namespace pegasus
