/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool-api/perf_counter.h>
#include <dsn/tool-api/perf_counters.h>
#include <dsn/service_api_c.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/tool-api/task.h>
#include <dsn/cpp/json_helper.h>
#include "service_engine.h"

namespace dsn {

perf_counters::perf_counters(void)
{
    ::dsn::command_manager::instance().register_command(
        {"counter.list"},
        "counter.list - get the list of all counters",
        "counter.list",
        &perf_counters::list_counter);

    ::dsn::command_manager::instance().register_command(
        {"counter.value"},
        "counter.value - get current value of a specific counter",
        "counter.value app-name*section-name*counter-name",
        &perf_counters::get_counter_value);

    ::dsn::command_manager::instance().register_command(
        {"counter.sample"},
        "counter.sample - get latest sample of a specific counter",
        "counter.sample app-name*section-name*counter-name",
        &perf_counters::get_counter_sample);
}

perf_counters::~perf_counters(void) {}

perf_counter_ptr perf_counters::new_app_counter(const char *section,
                                                const char *name,
                                                dsn_perf_counter_type_t flags,
                                                const char *dsptr)
{
    dsn_app_info info;
    dsn_get_current_app_info(&info);
    return new_global_counter(info.name, section, name, flags, dsptr);
}

perf_counter_ptr perf_counters::new_global_counter(const char *app,
                                                   const char *section,
                                                   const char *name,
                                                   dsn_perf_counter_type_t flags,
                                                   const char *dsptr)
{
    std::string full_name;
    perf_counter::build_full_name(app, section, name, full_name);
    {
        utils::auto_write_lock l(_lock);
        auto it = _counters.find(full_name);
        if (it == _counters.end()) {
            perf_counter_ptr counter = _factory(app, section, name, flags, dsptr);
            _counters.emplace(std::piecewise_construct,
                              std::forward_as_tuple(full_name),
                              std::forward_as_tuple(counter));
            return counter;
        } else
            return nullptr;
    }
}

perf_counter_ptr perf_counters::get_app_counter(const char *section,
                                                const char *name,
                                                dsn_perf_counter_type_t flags,
                                                const char *dsptr,
                                                bool create_if_not_exist)
{
    auto cnode = dsn::task::get_current_node2();
    dassert(cnode != nullptr, "cannot get current service node!");
    return get_global_counter(cnode->name(), section, name, flags, dsptr, create_if_not_exist);
}

perf_counter_ptr perf_counters::get_global_counter(const char *app,
                                                   const char *section,
                                                   const char *name,
                                                   dsn_perf_counter_type_t flags,
                                                   const char *dsptr,
                                                   bool create_if_not_exist)
{
    std::string full_name;
    perf_counter::build_full_name(app, section, name, full_name);

    if (create_if_not_exist) {
        utils::auto_write_lock l(_lock);

        auto it = _counters.find(full_name);
        if (it == _counters.end()) {
            perf_counter_ptr counter = _factory(app, section, name, flags, dsptr);
            _counters.emplace(std::piecewise_construct,
                              std::forward_as_tuple(full_name),
                              std::forward_as_tuple(counter));
            return counter;
        } else {
            dassert(it->second->type() == flags,
                    "counters with the same name %s with differnt types, (%d) vs (%d)",
                    full_name.c_str(),
                    it->second->type(),
                    flags);
            return it->second;
        }
    } else {
        utils::auto_read_lock l(_lock);

        auto it = _counters.find(full_name);
        if (it == _counters.end())
            return nullptr;
        else
            return it->second;
    }
}

perf_counter_ptr perf_counters::get_counter(const char *full_name)
{
    utils::auto_read_lock l(_lock);

    auto it = _counters.find(full_name);
    if (it == _counters.end())
        return nullptr;
    else
        return it->second;
}

bool perf_counters::remove_counter(const char *full_name)
{
    {
        utils::auto_write_lock l(_lock);
        auto it = _counters.find(full_name);
        if (it == _counters.end())
            return false;
        else {
            _counters.erase(it);
        }
    }

    dinfo("performance counter %s is removed", full_name);
    return true;
}

void perf_counters::get_all_counters(std::vector<perf_counter_ptr> *counter_vec)
{
    counter_vec->clear();
    utils::auto_read_lock l(_lock);
    counter_vec->reserve(_counters.size());
    for (auto &cp : _counters) {
        counter_vec->push_back(cp.second);
    }
}

void perf_counters::register_factory(perf_counter::factory factory)
{
    utils::auto_write_lock l(_lock);
    _factory = factory;
}

std::string perf_counters::list_counter(const std::vector<std::string> &args)
{
    return perf_counters::instance().list_counter_internal(args);
}

struct counter_info
{
    std::string name;
    DEFINE_JSON_SERIALIZATION(name)
};

struct value_resp
{
    double val;
    uint64_t time;
    std::string counter_name;
    DEFINE_JSON_SERIALIZATION(val, time, counter_name)
};

struct sample_resp
{
    uint64_t val;
    uint64_t time;
    std::string counter_name;
    DEFINE_JSON_SERIALIZATION(val, time, counter_name)
};

std::string perf_counters::list_counter_internal(const std::vector<std::string> &args)
{
    // <app, <section, counter_info[] > > counters
    std::map<std::string, std::map<std::string, std::vector<counter_info>>> counters;
    std::map<std::string, std::vector<counter_info>> empty_m;
    std::vector<counter_info> empty_v;

    std::map<std::string, std::vector<counter_info>> *pp;
    std::vector<counter_info> *pv;

    {
        utils::auto_read_lock l(_lock);
        for (auto &c : _counters) {
            pp = &counters
                      .insert(
                          std::map<std::string, std::map<std::string, std::vector<counter_info>>>::
                              value_type(c.second->app(), empty_m))
                      .first->second;

            pv = &pp->insert(std::map<std::string, std::vector<counter_info>>::value_type(
                                 c.second->section(), empty_v))
                      .first->second;

            pv->push_back({c.second->name()});
        }
    }

    std::stringstream ss;
    dsn::json::json_encode(ss, counters);
    return ss.str();
}

std::string perf_counters::get_counter_value(const std::vector<std::string> &args)
{
    std::stringstream ss;

    uint64_t ts = dsn_now_ns();
    double value = 0;

    if (args.size() < 1) {
        value_resp{value, ts, std::string()}.encode_json_state(ss);
        return ss.str();
    }

    perf_counters &c = perf_counters::instance();
    auto counter = c.get_counter(args[0].c_str());

    if (counter) {
        if (counter->type() != COUNTER_TYPE_NUMBER_PERCENTILES) {
            value = counter->get_value();
        }
    }

    value_resp{value, ts, args[0]}.encode_json_state(ss);
    return ss.str();
}

std::string perf_counters::get_counter_sample(const std::vector<std::string> &args)
{
    std::stringstream ss;

    uint64_t ts = dsn_now_ns();
    uint64_t sample = 0;

    if (args.size() < 1) {
        sample_resp{sample, ts, std::string()}.encode_json_state(ss);
        return ss.str();
    }

    perf_counters &c = perf_counters::instance();
    auto counter = c.get_counter(args[0].c_str());

    if (counter) {
        sample = counter->get_latest_sample();
    }
    sample_resp{sample, ts, args[0]}.encode_json_state(ss);
    return ss.str();
}

} // end namespace
