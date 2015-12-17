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
# include "service_engine.h"

DSN_API dsn_handle_t dsn_perf_counter_create(const char* name, dsn_perf_counter_type_t type, const char* description)
{
    auto cnode = dsn::task::get_current_node2();
    dassert(cnode != nullptr, "cannot get current service node!");
    auto c = dsn::utils::perf_counters::instance().get_counter(cnode->name(), name, type, description, true);
    c->add_ref();
    return c.get();
}
DSN_API void dsn_perf_counter_remove(dsn_handle_t handle)
{
    auto sptr = reinterpret_cast<dsn::perf_counter*>(handle);
    if (dsn::utils::perf_counters::instance().remove_counter(sptr->section(), sptr->name()))
        sptr->release_ref();
    else
    {
        dassert(false, "cannot remove counter %s.%s as it is not found in our repo", sptr->section(), sptr->name());
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
    ::dsn::register_command("counter.list", "counter.list - get the list of all counters",
        "counter.list",
        &perf_counters::list_counter
        );

    ::dsn::register_command("counter.query", "counter.query - get current value of a specific counter",
        "counter.query [counter]",
        &perf_counters::query_counter
        );
}

perf_counters::~perf_counters(void)
{
}

perf_counter_ptr perf_counters::get_counter(const char *section, const char *name, dsn_perf_counter_type_t flags, const char *dsptr, bool create_if_not_exist /*= false*/)
{
    char section_name[512] = "";
    //::GetModuleBaseNameA(::GetCurrentProcess(), ::GetModuleHandleA(nullptr), section_name, 256);
    //strcat(section_name, ".");
    strcat(section_name, section);
    if (create_if_not_exist)
    {
        auto_write_lock l(_lock);

        auto it = _counters.find(section_name);
        if (it == _counters.end())
        {
            it = _counters.emplace(std::piecewise_construct, std::forward_as_tuple(section_name), std::forward_as_tuple(same_section_counters{})).first;
        }

        auto it2 = it->second.find(name);
        if (it2 == it->second.end())
        {
            perf_counter_ptr counter = _factory(section_name, name, flags, dsptr);
            it->second.emplace(std::piecewise_construct, std::forward_as_tuple(name), std::forward_as_tuple(counter));
            return counter;
        }
        else
        {
            dassert (it2->second->type() == flags, "counters with the same name %s.%s with differnt types", section_name, name);
            return it2->second;
        }
    }
    else
    {
        auto_read_lock l(_lock);

        auto it = _counters.find(section_name);
        if (it == _counters.end())
            return nullptr;

        auto it2 = it->second.find(name);
        if (it2 == it->second.end())
            return nullptr;

        return it2->second;
    }
}

bool perf_counters::remove_counter(const char* section, const char* name)
{
    char section_name[512] = "";
    //::GetModuleBaseNameA(::GetCurrentProcess(), ::GetModuleHandleA(nullptr), section_name, 256);
    //strcat(section_name, ".");
    strcat(section_name, section);

    auto_write_lock l(_lock);

    auto it = _counters.find(section_name);
    if (it == _counters.end())
        return false;

    auto it2 = it->second.find(name);
    if (it2 == it->second.end())
        return false;

    it->second.erase(it2);
    if (it->second.size() == 0)
        _counters.erase(it);

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
    auto_read_lock l(_lock);
    std::stringstream ss;
    ss << "[";
    bool first_flag = 0;
    for (auto& section : _counters)
    {
        for (auto& counter : section.second)
        {
            if (!first_flag)
                first_flag = 1;
            else
                ss << ",";
            ss << "\"" << counter.first << "\"";
        }
    }
    ss << "]";
    return ss.str();
}

std::string perf_counters::query_counter(const std::vector<std::string>& args)
{
    std::stringstream ss;

    if (args.size() < 1)
    {
        ss << "unenough arguments" << std::endl;
        return ss.str();
    }

    utils::perf_counters& c = utils::perf_counters::instance();
    auto counter = c.get_counter(args[0].c_str(), COUNTER_TYPE_NUMBER, "", false);

    if (counter)
        ss << counter->get_current_sample();
    else
        ss << (uint64_t)(0);

    return ss.str();
}

} } // end namespace

