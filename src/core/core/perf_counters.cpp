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
    auto c = dsn::perf_counters::instance().get_counter(cnode->name(), section, name, type, description, true);
    c->add_ref();
    return c.get();
}

DSN_API void dsn_perf_counter_remove(dsn_handle_t handle)
{
    auto sptr = reinterpret_cast<dsn::perf_counter*>(handle);
    if (dsn::perf_counters::instance().remove_counter(sptr->full_name()))
        sptr->release_ref();
    else
    {
        dwarn("cannot remove counter %s as it is not found in our repo", sptr->full_name());
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
DSN_API uint64_t dsn_perf_counter_get_integer_value(dsn_handle_t handle)
{
    return reinterpret_cast<dsn::perf_counter*>(handle)->get_integer_value();
}
DSN_API double dsn_perf_counter_get_percentile(dsn_handle_t handle, dsn_perf_counter_percentile_type_t type)
{
    return reinterpret_cast<dsn::perf_counter*>(handle)->get_percentile(type);
}

namespace dsn {
    
perf_counters::perf_counters(void)
{
    _max_counter_count = dsn_config_get_value_uint64(
        "core",
        "perf_counter_max_count",
        10000,
        "maximum number of performance counters"
        );

    dassert(_max_counter_count > 0 && _max_counter_count < 1000000,
        "invalid given perf_counter_max_count value %" PRIu64, 
        _max_counter_count
        );
    _quick_counters = new perf_counter*[_max_counter_count];
    memset((void*)_quick_counters, 0, sizeof(perf_counter*) * _max_counter_count);

    // index << 32 | version
    // zero is reserved for invalid
    for (uint64_t i = 1; i < _max_counter_count; i++)
    {
        _quick_counters_empty_slots.push((i << 32) | 0x0);
    }

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

    ::dsn::register_command("counter.valuei",
        "counter.valuei - get current value of a specific counter",
        "counter.valuei counter-index",
        &perf_counters::get_counter_value_i
        );

    ::dsn::register_command("counter.samplei",
        "counter.samplei - get latest sample of a specific counter",
        "counter.samplei counter-index",
        &perf_counters::get_counter_sample_i
        );

    ::dsn::register_command("counter.getindex",
        "counter.getindex - get index of a list of counters by name",
        "counter.getindex app-name1*section-name1*counter-name1 app-name2*section-name2*counter-name2 ...",
        &perf_counters::get_counter_index
        );

}

perf_counters::~perf_counters(void)
{
    delete[] _quick_counters;
}

perf_counter_ptr perf_counters::get_counter(const char* app, const char *section, const char *name, dsn_perf_counter_type_t flags, const char *dsptr, bool create_if_not_exist /*= false*/)
{
    std::string full_name;
    perf_counter::build_full_name(app, section, name, full_name);

    if (create_if_not_exist)
    {
        utils::auto_write_lock l(_lock);

        auto it = _counters.find(full_name);
        if (it == _counters.end())
        {
            dassert(_quick_counters_empty_slots.size() > 0,
                "no more slots for perf counters, please increase [core] perf_counter_max_count"
                );

            uint64_t idx = _quick_counters_empty_slots.front();
            _quick_counters_empty_slots.pop();

            // increase version 
            idx++;

            perf_counter_ptr counter = _factory(app, section, name, flags, dsptr);
            counter->_index = idx; // index << 32 | version

            _quick_counters[idx >> 32] = counter.get();
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
        utils::auto_read_lock l(_lock);

        auto it = _counters.find(full_name);
        if (it == _counters.end())
            return nullptr;
        else
            return it->second;
    }
}

perf_counter_ptr perf_counters::get_counter(const char* full_name)
{
    utils::auto_read_lock l(_lock);

    auto it = _counters.find(full_name);
    if (it == _counters.end())
        return nullptr;
    else
        return it->second;
}
perf_counter_ptr perf_counters::get_counter(uint64_t index)
{
    utils::auto_read_lock l(_lock);

    auto slot = (index >> 32);
    if (slot >= _max_counter_count)
        return nullptr;

    if (_quick_counters[slot] && _quick_counters[slot]->index() == index)
    {
        return _quick_counters[slot];
    }
    else
    {
        return nullptr;
    }
}

bool perf_counters::remove_counter(const char* full_name)
{
    {
        utils::auto_write_lock l(_lock);

        auto it = _counters.find(full_name);
        if (it == _counters.end())
            return false;
        else
        {
            _quick_counters_empty_slots.push(it->second->index());
            auto& c = _quick_counters[it->second->index() >> 32];
            dassert(c == it->second.get(), 
                "invalid perf counter pointer stored in quick access array");

            c = nullptr;
            _counters.erase(it);
        }
    }
    
    dinfo("performance counter %s is removed", full_name);
    return true;
}

void perf_counters::register_factory(perf_counter::factory factory)
{
    utils::auto_write_lock l(_lock);
    _factory = factory;
}


std::string perf_counters::list_counter(const std::vector<std::string>& args)
{
    return perf_counters::instance().list_counter_internal(args);
}

struct counter_info
{
    std::string name;
    uint64_t    index;

    DEFINE_JSON_SERIALIZATION(name, index)
};

struct value_resp {
    double val;
    uint64_t time;
    DEFINE_JSON_SERIALIZATION(val, time)
};

struct sample_resp {
    uint64_t val;
    uint64_t time;
    DEFINE_JSON_SERIALIZATION(time, val)
};

std::string perf_counters::list_counter_internal(const std::vector<std::string>& args)
{
    // <app, <section, counter_info[] > > counters
    std::map<std::string, std::map< std::string, std::vector<counter_info> > > counters;
    std::map< std::string, std::vector<counter_info> > empty_m;
    std::vector<counter_info> empty_v;

    std::map< std::string, std::vector<counter_info> >* pp;
    std::vector<counter_info>* pv;

    {
        utils::auto_read_lock l(_lock);
        for (auto& c : _counters)
        {
            pp = &counters.insert(
                std::map<std::string, std::map< std::string, std::vector<counter_info> > >::value_type(
                c.second->app(),
                empty_m
                )
                ).first->second;

            pv = &pp->insert(
                std::map< std::string, std::vector<counter_info> >::value_type(
                c.second->section(),
                empty_v
                )
                ).first->second;

            pv->push_back({ c.second->name(), c.second->index() });
        }
    }

    std::stringstream ss;
    std::json_encode(ss, counters);
    return ss.str();
}

std::string perf_counters::get_counter_value(const std::vector<std::string>& args)
{
    std::stringstream ss;

    uint64_t ts = dsn_now_ns();
    double value = 0;

    if (args.size() < 1)
    {
        value_resp{ value, ts }.json_state(ss);
        return ss.str();
    }

    perf_counters& c = perf_counters::instance();
    auto counter = c.get_counter(args[0].c_str());

    if (counter && counter->type() != COUNTER_TYPE_NUMBER_PERCENTILES)
        value =  counter->get_value();

    value_resp{ value, ts }.json_state(ss);
    return ss.str();
}


std::string perf_counters::get_counter_sample(const std::vector<std::string>& args)
{
    std::stringstream ss;

    uint64_t ts = dsn_now_ns();
    uint64_t sample = 0;

    if (args.size() < 1)
    {
        sample_resp{ sample, ts }.json_state(ss);
        return ss.str();
    }

    perf_counters& c = perf_counters::instance();
    auto counter = c.get_counter(args[0].c_str());

    if (counter)
        sample = counter->get_latest_sample();

    sample_resp{ sample, ts }.json_state(ss);
    return ss.str();
}


std::string perf_counters::get_counter_value_i(const std::vector<std::string>& args)
{
    std::stringstream ss;

    uint64_t ts = dsn_now_ns();
    double value = 0;

    if (args.size() < 1)
    {
        value_resp{ value, ts }.json_state(ss);
        return ss.str();
    }

    uint64_t idx = atoll(args[0].c_str());

    perf_counters& c = perf_counters::instance();
    auto counter = c.get_counter(idx);

    if (counter && counter->type() != COUNTER_TYPE_NUMBER_PERCENTILES)
        value = counter->get_value();

    value_resp{ value, ts }.json_state(ss);
    return ss.str();
}


std::string perf_counters::get_counter_sample_i(const std::vector<std::string>& args)
{
    std::stringstream ss;

    uint64_t ts = dsn_now_ns();
    uint64_t sample = 0;

    if (args.size() < 1)
    {
        sample_resp{ sample, ts }.json_state(ss);
        return ss.str();
    }

    uint64_t idx = atoll(args[0].c_str());

    perf_counters& c = perf_counters::instance();
    auto counter = c.get_counter(idx);

    if (counter)
        sample = counter->get_latest_sample();

    sample_resp{ sample, ts }.json_state(ss);
    return ss.str();
}

std::string perf_counters::get_counter_index(const std::vector<std::string>& args)
{
    std::stringstream ss;

    std::vector<uint64_t> counter_index_list;

    for (auto counter_name : args)
    {
        perf_counters& c = perf_counters::instance();
        auto counter = c.get_counter(counter_name.c_str());
        if (counter)
            counter_index_list.push_back(counter->index());
        else
            counter_index_list.push_back(0);
    }
    
    std::json_encode(ss, counter_index_list);
    return ss.str();
}

} // end namespace

