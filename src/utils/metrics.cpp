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

#include "utils/metrics.h"

#include "utils/api_utilities.h"
#include "utils/rand.h"

#include "shared_io_service.h"

namespace dsn {

metric_entity::metric_entity(const std::string &id, attr_map &&attrs)
    : _id(id), _lock(), _attrs(std::move(attrs)), _metrics()
{
}

metric_entity::~metric_entity()
{
    // We have to wait for all of close operations to be finished. Waiting for close operations to
    // be finished in the destructor of each metirc may lead to memory leak detected in ASAN test
    // for dsn_utils_test, since the percentile is also referenced by shared_io_service which is
    // still alive without being destructed after ASAN test for dsn_utils_test is finished.
    close(close_option::kWait);
}

void metric_entity::close(close_option option)
{
    utils::auto_write_lock l(_lock);

    // The reason why each metric is closed in the entity rather than in the destructor of each
    // metric is that close() for the metric will return immediately without waiting for any close
    // operation to be finished.
    //
    // Thus, to close all metrics owned by an entity, it's more efficient to firstly issue a close
    // request for all metrics; then, just wait for all of the close operations to be finished.
    // It's inefficient to wait for each metric to be closed one by one.
    for (auto &m : _metrics) {
        if (m.second->prototype()->type() == metric_type::kPercentile) {
            auto p = down_cast<closeable_metric *>(m.second.get());
            p->close();
        }
    }

    if (option == close_option::kNoWait) {
        return;
    }

    // Wait for all of the close operations to be finished.
    for (auto &m : _metrics) {
        if (m.second->prototype()->type() == metric_type::kPercentile) {
            auto p = down_cast<closeable_metric *>(m.second.get());
            p->wait();
        }
    }
}

metric_entity::attr_map metric_entity::attributes() const
{
    utils::auto_read_lock l(_lock);
    return _attrs;
}

metric_entity::metric_map metric_entity::metrics() const
{
    utils::auto_read_lock l(_lock);
    return _metrics;
}

void metric_entity::set_attributes(attr_map &&attrs)
{
    utils::auto_write_lock l(_lock);
    _attrs = std::move(attrs);
}

metric_entity_ptr metric_entity_prototype::instantiate(const std::string &id,
                                                       metric_entity::attr_map attrs) const
{
    CHECK(attrs.find("entity") == attrs.end(), "{}'s attribute \"entity\" is reserved", id);

    attrs["entity"] = _name;
    return metric_registry::instance().find_or_create_entity(id, std::move(attrs));
}

metric_entity_ptr metric_entity_prototype::instantiate(const std::string &id) const
{
    return instantiate(id, {});
}

metric_entity_prototype::metric_entity_prototype(const char *name) : _name(name) {}

metric_entity_prototype::~metric_entity_prototype() {}

metric_registry::metric_registry() : _lock(), _entities()
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
    utils::auto_write_lock l(_lock);

    // Once the registery is chosen to be destructed, all of the entities and metrics owned by it
    // will no longer be needed.
    //
    // The reason why each entity is closed in the registery rather than in the destructor of each
    // entity is that close(kNoWait) for the entity will return immediately without waiting for any
    // close operation to be finished.
    //
    // Thus, to close all entities owned by a registery, it's more efficient to firstly issue a
    // close request for all entities; then, just wait for all of the close operations to be
    // finished in the destructor of each entity. It's inefficient to wait for each entity to be
    // closed one by one.
    for (auto &entity : _entities) {
        entity.second->close(metric_entity::close_option::kNoWait);
    }
}

metric_registry::entity_map metric_registry::entities() const
{
    utils::auto_read_lock l(_lock);
    return _entities;
}

metric_entity_ptr metric_registry::find_or_create_entity(const std::string &id,
                                                         metric_entity::attr_map &&attrs)
{
    utils::auto_write_lock l(_lock);

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
    CHECK_GT(interval_ms, 0);

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
      _completed(),
      _timer(new boost::asio::deadline_timer(tools::shared_io_service::instance().ios))
{
    _timer->expires_from_now(boost::posix_time::milliseconds(_initial_delay_ms));
    _timer->async_wait(std::bind(&percentile_timer::on_timer, this, std::placeholders::_1));
}

void percentile_timer::close()
{
    // If the timer has already expired when cancel() is called, then the handlers for asynchronous
    // wait operations will:
    // * have already been invoked; or
    // * have been queued for invocation in the near future.
    //
    // These handlers can no longer be cancelled, and therefore are passed an error code that
    // indicates the successful completion of the wait operation. Thus set the state of timer to
    // kClosing to tell on_timer() that the timer should be closed even if it is not called with
    // operation_canceled.
    auto expected_state = state::kRunning;
    if (_state.compare_exchange_strong(expected_state, state::kClosing)) {
        _timer->cancel();
    }
}

void percentile_timer::wait() { _completed.wait(); }

void percentile_timer::on_close()
{
    _on_close();
    _completed.notify();
}

void percentile_timer::on_timer(const boost::system::error_code &ec)
{
// This macro is defined for the case that handlers for asynchronous wait operations are no
// longer cancelled. It just checks the internal state atomically (since close() can also be
// called simultaneously) for kClosing; once it's matched, it will stop the timer by not executing
// any future handler.
#define TRY_PROCESS_TIMER_CLOSING()                                                                \
    do {                                                                                           \
        auto expected_state = state::kClosing;                                                     \
        if (_state.compare_exchange_strong(expected_state, state::kClosed)) {                      \
            on_close();                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

    if (dsn_unlikely(!!ec)) {
        CHECK_EQ_MSG(ec,
                     boost::system::errc::operation_canceled,
                     "failed to exec on_timer with an error that cannot be handled: {}",
                     ec.message());

        // Cancel can only be launched by close().
        auto expected_state = state::kClosing;
        CHECK(_state.compare_exchange_strong(expected_state, state::kClosed),
              "wrong state for percentile_timer: {}, while expecting closing state",
              static_cast<int>(expected_state));
        on_close();

        return;
    }

    TRY_PROCESS_TIMER_CLOSING();
    _on_exec();

    TRY_PROCESS_TIMER_CLOSING();
    _timer->expires_from_now(boost::posix_time::milliseconds(_interval_ms));
    _timer->async_wait(std::bind(&percentile_timer::on_timer, this, std::placeholders::_1));
#undef TRY_PROCESS_TIMER_CLOSING
}

} // namespace dsn
