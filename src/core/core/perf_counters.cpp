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

namespace dsn { namespace utils {
    
perf_counters::perf_counters(void)
{
    ::dsn::register_command("list_counter", "list_counter - get engine internal information",
        "list_counter",
        &perf_counters::list_counter
        );

    ::dsn::register_command("query_counter", "query_counter - get engine internal information",
        "query_counter [counter]",
        &perf_counters::query_counter
        );
}

perf_counters::~perf_counters(void)
{
}

perf_counter_ptr perf_counters::get_counter(const char *section, const char *name, perf_counter_type flags, const char *dsptr, bool create_if_not_exist /*= false*/)
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
            same_section_counters sc;
            it = _counters.insert(all_counters::value_type(section_name, sc)).first;
        }

        auto it2 = it->second.find(name);
        if (it2 == it->second.end())
        {
            perf_counter_ptr counter(_factory(section_name, name, flags,dsptr));
            it->second.insert(same_section_counters::value_type(name, std::make_pair(counter, flags)));
            return counter;
        }
        else
        {
            dassert (it2->second.second == flags, "counters with the same name %s.%s with differnt types", section_name, name);
            return it2->second.first;
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

        return it2->second.first;
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
    std::stringstream ss;

    utils::perf_counters& c = utils::perf_counters::instance();
    auto counters = c.get_all_counters();
    ss << "[";
    bool first_flag = 0;
    for (auto section : counters)
    {
        for (auto counter : section.second)
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
    ss << counter->get_current_sample();
    return ss.str();
}

} } // end namespace

