// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "perf_counter/perf_counter_atomic.h"

#include <boost/asio/basic_deadline_timer.hpp>
#include <cstdlib>
#include <functional>

#include "boost/asio/deadline_timer.hpp"
#include "boost/asio/detail/impl/epoll_reactor.hpp"
#include "boost/asio/detail/impl/scheduler.ipp"
#include "boost/asio/detail/impl/service_registry.hpp"
#include "boost/asio/detail/impl/timer_queue_ptime.ipp"
#include "boost/asio/impl/any_io_executor.ipp"
#include "boost/asio/impl/io_context.hpp"
#include "boost/date_time/posix_time/posix_time_duration.hpp"
#include "boost/system/detail/errc.hpp"
#include "boost/system/detail/error_code.hpp"
#include "boost/system/detail/error_condition.hpp"
#include "utils/flags.h"
#include "utils/shared_io_service.h"

DSN_DEFINE_int32(components.pegasus_perf_counter_number_percentile_atomic,
                 counter_computation_interval_seconds,
                 10,
                 "The interval seconds of the system to compute the percentiles of the "
                 "pegasus_perf_counter_number_percentile_atomic counters");

namespace dsn {
perf_counter_number_percentile_atomic::perf_counter_number_percentile_atomic(
    const char *app,
    const char *section,
    const char *name,
    dsn_perf_counter_type_t type,
    const char *dsptr,
    bool use_timer)
    : perf_counter(app, section, name, type, dsptr), _tail(0)
{
    _results[COUNTER_PERCENTILE_50] = 0;
    _results[COUNTER_PERCENTILE_90] = 0;
    _results[COUNTER_PERCENTILE_95] = 0;
    _results[COUNTER_PERCENTILE_99] = 0;
    _results[COUNTER_PERCENTILE_999] = 0;

    if (!use_timer) {
        return;
    }

    _timer.reset(new boost::asio::deadline_timer(tools::shared_io_service::instance().ios));
    _timer->expires_from_now(
        boost::posix_time::seconds(::rand() % FLAGS_counter_computation_interval_seconds + 1));
    _timer->async_wait(std::bind(
        &perf_counter_number_percentile_atomic::on_timer, this, _timer, std::placeholders::_1));
}

void perf_counter_number_percentile_atomic::on_timer(
    std::shared_ptr<boost::asio::deadline_timer> timer, const boost::system::error_code &ec)
{
    // as the callback is not in tls context, so the log system calls like LOG_INFO, CHECK
    // will cause a lock
    if (!ec) {
        calc(std::make_shared<compute_context>());

        timer->expires_from_now(
            boost::posix_time::seconds(FLAGS_counter_computation_interval_seconds));
        timer->async_wait(std::bind(
            &perf_counter_number_percentile_atomic::on_timer, this, timer, std::placeholders::_1));
    } else if (boost::system::errc::operation_canceled != ec) {
        CHECK(false, "on_timer error!!!");
    }
}

} // namespace dsn
