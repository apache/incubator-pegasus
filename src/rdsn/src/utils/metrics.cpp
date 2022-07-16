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

metric_entity::metric_entity(const std::string &id, attr_map &&attrs)
    : _id(id), _attrs(std::move(attrs))
{
}

metric_entity::~metric_entity() {}

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

metric_registry::~metric_registry() {}

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

uint64_t percentile_timer::generate_initial_delay_ms(uint64_t interval_ms)
{
    dcheck_gt(interval_ms, 0);

    if (interval_ms < 1000) {
        return rand::next_u64() % interval_ms + 50;
    }

    uint64_t interval_seconds = interval_ms / 1000;
    return (rand::next_u64() % interval_seconds + 1) * 1000 + rand::next_u64() % 1000;
}

percentile_timer::percentile_timer(uint64_t interval_ms, exec_fn exec)
    : _initial_delay_ms(generate_initial_delay_ms(interval_ms)),
      _interval_ms(interval_ms),
      _exec(exec),
      _timer(new boost::asio::deadline_timer(tools::shared_io_service::instance().ios))
{
    _timer->expires_from_now(boost::posix_time::milliseconds(_initial_delay_ms));
    _timer->async_wait(std::bind(&percentile_timer::on_timer, this, std::placeholders::_1));
}

void percentile_timer::on_timer(const boost::system::error_code &ec)
{
    if (dsn_unlikely(!!ec)) {
        dassert_f(ec == boost::system::errc::operation_canceled,
                  "failed to exec on_timer with an error that cannot be handled: {}",
                  ec.message());
        return;
    }

    _exec();

    _timer->expires_from_now(boost::posix_time::milliseconds(_interval_ms));
    _timer->async_wait(std::bind(&percentile_timer::on_timer, this, std::placeholders::_1));
}

} // namespace dsn
