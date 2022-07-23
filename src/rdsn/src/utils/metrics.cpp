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

#include <dsn/utility/metrics.h>

#include <dsn/c/api_utilities.h>
#include <dsn/utility/rand.h>

#include "shared_io_service.h"

namespace dsn {

std::set<kth_percentile_type> get_all_kth_percentile_types()
{
    std::set<kth_percentile_type> all_types;
    for (size_t i = 0; i < static_cast<size_t>(kth_percentile_type::COUNT); ++i) {
        all_types.insert(static_cast<kth_percentile_type>(i));
    }
    return all_types;
}

metric_entity::metric_entity(const std::string &id, attr_map &&attrs)
    : _id(id), _attrs(std::move(attrs))
{
}

metric_entity::~metric_entity() {}

void metric_entity::close()
{
    std::lock_guard<std::mutex> guard(_mtx);

    for (auto &m : _metrics) {
        if (m.second->prototype()->type() == metric_type::kPercentile) {
            auto p = down_cast<closeable_metric *>(m.second.get());
            p->close();
        }
    }
}

metric_entity::attr_map metric_entity::attributes() const
{
    std::lock_guard<std::mutex> guard(_mtx);
    return _attrs;
}

metric_entity::metric_map metric_entity::metrics() const
{
    std::lock_guard<std::mutex> guard(_mtx);
    return _metrics;
}

void metric_entity::set_attributes(attr_map &&attrs)
{
    std::lock_guard<std::mutex> guard(_mtx);
    _attrs = std::move(attrs);
}

metric_entity_ptr metric_entity_prototype::instantiate(const std::string &id,
                                                       metric_entity::attr_map attrs) const
{
    dassert_f(attrs.find("entity") == attrs.end(), "{}'s attribute \"entity\" is reserved", id);

    attrs["entity"] = _name;
    return metric_registry::instance().find_or_create_entity(id, std::move(attrs));
}

metric_entity_ptr metric_entity_prototype::instantiate(const std::string &id) const
{
    return instantiate(id, {});
}

metric_entity_prototype::metric_entity_prototype(const char *name) : _name(name) {}

metric_entity_prototype::~metric_entity_prototype() {}

metric_registry::metric_registry()
{
    // We should ensure that metric_registry is destructed before shared_io_service is destructed.
    // Once shared_io_service is destructed before metric_registry is destructed,
    // boost::asio::io_service needed by metrics in metric_registry such as percentile_timer will
    // be released firstly, then will lead to heap-use-after-free error since percentiles in
    // metric_registry are still running but the resources they needed have been released.
    tools::shared_io_service::instance();
}

metric_registry::~metric_registry()
{
    std::lock_guard<std::mutex> guard(_mtx);

    for (auto &entity : _entities) {
        entity.second->close();
    }
}

metric_registry::entity_map metric_registry::entities() const
{
    std::lock_guard<std::mutex> guard(_mtx);

    return _entities;
}

metric_entity_ptr metric_registry::find_or_create_entity(const std::string &id,
                                                         metric_entity::attr_map &&attrs)
{
    std::lock_guard<std::mutex> guard(_mtx);

    entity_map::const_iterator iter = _entities.find(id);

    metric_entity_ptr entity;
    if (iter == _entities.end()) {
        entity = new metric_entity(id, std::move(attrs));
        _entities[id] = entity;
    } else {
        iter->second->set_attributes(std::move(attrs));
        entity = iter->second;
    }

    return entity;
}

metric_prototype::metric_prototype(const ctor_args &args) : _args(args) {}

metric_prototype::~metric_prototype() {}

metric::metric(const metric_prototype *prototype) : _prototype(prototype) {}

closeable_metric::closeable_metric(const metric_prototype *prototype) : metric(prototype) {}

uint64_t percentile_timer::generate_initial_delay_ms(uint64_t interval_ms)
{
    dcheck_gt(interval_ms, 0);

    if (interval_ms < 1000) {
        return rand::next_u64() % interval_ms + 50;
    }

    uint64_t interval_seconds = interval_ms / 1000;
    return (rand::next_u64() % interval_seconds + 1) * 1000 + rand::next_u64() % 1000;
}

percentile_timer::percentile_timer(uint64_t interval_ms, on_exec_fn on_exec, on_close_fn on_close)
    : _initial_delay_ms(generate_initial_delay_ms(interval_ms)),
      _interval_ms(interval_ms),
      _on_exec(on_exec),
      _on_close(on_close),
      _state(state::kRunning),
      _timer(new boost::asio::deadline_timer(tools::shared_io_service::instance().ios))
{
    _timer->expires_from_now(boost::posix_time::milliseconds(_initial_delay_ms));
    _timer->async_wait(std::bind(&percentile_timer::on_timer, this, std::placeholders::_1));
}

void percentile_timer::close()
{
    auto expected_state = state::kRunning;
    if (_state.compare_exchange_strong(expected_state, state::kClosing)) {
        _timer->cancel();
    }
}

void percentile_timer::on_timer(const boost::system::error_code &ec)
{
#define TRY_PROCESS_TIMER_CLOSING()                                                                \
    do {                                                                                           \
        auto expected_state = state::kClosing;                                                     \
        if (_state.compare_exchange_strong(expected_state, state::kClosed)) {                      \
            _on_close();                                                                           \
            return;                                                                                \
        }                                                                                          \
    } while (0)

    if (dsn_unlikely(!!ec)) {
        dassert_f(ec == boost::system::errc::operation_canceled,
                  "failed to exec on_timer with an error that cannot be handled: {}",
                  ec.message());

        auto expected_state = state::kClosing;
        dassert_f(_state.compare_exchange_strong(expected_state, state::kClosed),
                  "wrong state for percentile_timer: {}, while expecting closing state",
                  static_cast<int>(expected_state));
        _on_close();

        return;
    }

    TRY_PROCESS_TIMER_CLOSING();
    _on_exec();

    TRY_PROCESS_TIMER_CLOSING();
    _timer->expires_from_now(boost::posix_time::milliseconds(_interval_ms));
    _timer->async_wait(std::bind(&percentile_timer::on_timer, this, std::placeholders::_1));
}

template <>
gauge<int64_t>::gauge(const metric_prototype *prototype) : gauge(prototype, 0)
{
}

template <>
gauge<double>::gauge(const metric_prototype *prototype) : gauge(prototype, 0.0)
{
}

} // namespace dsn
