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

# include <dsn/internal/perf_counters.h>
# include <dsn/service_api_c.h>
# include <dsn/internal/command.h>
# include <dsn/internal/task.h>
# include <dsn/cpp/json_helper.h>
# include "service_engine.h"

DSN_API dsn_handle_t dsn_perf_counter_create(const char* section, const char* name, dsn_perf_counter_type_t type, const char* description)
{
    auto cnode = dsn::task::get_current_node2();
    dassert(cnode != nullptr, "cannot get current service node!");
    auto c = dsn::utils::perf_counters::instance().get_counter(cnode->name(), section, name, type, description, true);
    c->add_ref();
    return c.get();
}
DSN_API void dsn_perf_counter_remove(dsn_handle_t handle)
{
    auto sptr = reinterpret_cast<dsn::perf_counter*>(handle);
    if (dsn::utils::perf_counters::instance().remove_counter(sptr->full_name()))
        sptr->release_ref();
    else
    {
        dwarn("cannot remove counter %s.%s as it is not found in our repo", sptr->section(), sptr->name());
    }
}

DSN_API void dsn_perf_counter_increment(dsn_handle_t handle)
{
    reinterpret_cast<dsn::perf_counter*>(handle)->increment();
}

DSN_API void dsn_perf_counter_decrement(dsn_handle_t handle)
{
    reinterpret_cast<dsn::perf_counter*>(handle)->decrement();
}
DSN_API void dsn_perf_counter_add(dsn_handle_t handle, uint64_t val)
{
    reinterpret_cast<dsn::perf_counter*>(handle)->add(val);
}
DSN_API void dsn_perf_counter_set(dsn_handle_t handle, uint64_t val)
{
    reinterpret_cast<dsn::perf_counter*>(handle)->set(val);
}
DSN_API double dsn_perf_counter_get_value(dsn_handle_t handle)
{
    return reinterpret_cast<dsn::perf_counter*>(handle)->get_value();
}
DSN_API double dsn_perf_counter_get_percentile(dsn_handle_t handle, dsn_perf_counter_percentile_type_t type)
{
    return reinterpret_cast<dsn::perf_counter*>(handle)->get_percentile(type);
}

namespace dsn { namespace utils {
    
perf_counters::perf_counters(void)
{
    ::dsn::register_command("counter.list", 
        "counter.list - get the list of all counters",
        "counter.list",
        &perf_counters::list_counter
        );

    ::dsn::register_command("counter.value", 
        "counter.value - get current value of a specific counter",
        "counter.value app-name*section-name*counter-name",
        &perf_counters::get_counter_value
        );

    ::dsn::register_command("counter.sample",
        "counter.sample - get latest sample of a specific counter",
        "counter.sample app-name*section-name*counter-name",
        &perf_counters::get_counter_sample
        );
}

perf_counters::~perf_counters(void)
{
}

perf_counter_ptr perf_counters::get_counter(const char* app, const char *section, const char *name, dsn_perf_counter_type_t flags, const char *dsptr, bool create_if_not_exist /*= false*/)
{
    std::string full_name;
    perf_counter::build_full_name(app, section, name, full_name);

    if (create_if_not_exist)
    {
        auto_write_lock l(_lock);

        auto it = _counters.find(full_name);
        if (it == _counters.end())
        {
            perf_counter_ptr counter = _factory(app, section, name, flags, dsptr);
            _counters.emplace(std::piecewise_construct, std::forward_as_tuple(full_name), std::forward_as_tuple(counter));
            return counter;
        }
        else
        {
            dassert (it->second->type() == flags,
                "counters with the same name %s with differnt types",
                full_name.c_str()
                );
            return it->second;
        }
    }
    else
    {
        auto_read_lock l(_lock);

        auto it = _counters.find(full_name);
        if (it == _counters.end())
            return nullptr;
        else
            return it->second;
    }
}

perf_counter_ptr perf_counters::get_counter(const char* full_name)
{
    auto_read_lock l(_lock);

    auto it = _counters.find(full_name);
    if (it == _counters.end())
        return nullptr;
    else
        return it->second;
}

bool perf_counters::remove_counter(const char* full_name)
{
    {
        auto_write_lock l(_lock);

        auto it = _counters.find(full_name);
        if (it == _counters.end())
            return false;
        else
        {
            _counters.erase(it);
        }
    }
    
    dinfo("performance counter %s is removed", full_name);
    return true;
}

void perf_counters::register_factory(perf_counter::factory factory)
{
    auto_write_lock l(_lock);
    _factory = factory;
}


std::string perf_counters::list_counter(const std::vector<std::string>& args)
{
    return utils::perf_counters::instance().list_counter_internal(args);
}

std::string perf_counters::list_counter_internal(const std::vector<std::string>& args)
{
    // <app, <section, names[] > > counters
    std::map<std::string, std::map< std::string, std::vector<std::string> > > counters;
    std::map< std::string, std::vector<std::string> > empty_m;
    std::vector<std::string> empty_v;

    std::map< std::string, std::vector<std::string> >* pp;
    std::vector<std::string>* pv;

    {
        auto_read_lock l(_lock);
        for (auto& c : _counters)
        {
            pp = &counters.insert(
                std::map<std::string, std::map< std::string, std::vector<std::string> > >::value_type(
                c.second->app(),
                empty_m
                )
                ).first->second;

            pv = &pp->insert(
                std::map< std::string, std::vector<std::string> >::value_type(
                c.second->section(),
                empty_v
                )
                ).first->second;

            pv->push_back(c.second->name());
        }
    }

    std::stringstream ss;
    std::json_encode(ss, counters);
    return ss.str();
}

std::string perf_counters::get_counter_value(const std::vector<std::string>& args)
{
    std::stringstream ss;

    if (args.size() < 1)
    {
        ss << 0;
        return ss.str();
    }

    utils::perf_counters& c = utils::perf_counters::instance();
    auto counter = c.get_counter(args[0].c_str());

    if (counter && counter->type() != COUNTER_TYPE_NUMBER_PERCENTILES)
        ss << counter->get_value();
    else
        ss << 0;

    return ss.str();
}


std::string perf_counters::get_counter_sample(const std::vector<std::string>& args)
{
    std::stringstream ss;

    if (args.size() < 1)
    {
        ss << 0;
        return ss.str();
    }

    utils::perf_counters& c = utils::perf_counters::instance();
    auto counter = c.get_counter(args[0].c_str());

    if (counter)
        ss << counter->get_current_sample();
    else
        ss << 0;

    return ss.str();
}

} } // end namespace

