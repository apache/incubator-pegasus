# include <rdsn/internal/perf_counters.h>
# include <rdsn/internal/logging.h>

namespace rdsn { namespace utils {
    
perf_counters::perf_counters(void)
{
}

perf_counters::~perf_counters(void)
{
}

perf_counter_ptr perf_counters::get_counter(const char *section, const char *name, perf_counter_type flags, bool create_if_not_exist /*= false*/)
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
            perf_counter_ptr counter(_factory(section_name, name, flags));
            it->second.insert(same_section_counters::value_type(name, std::make_pair(counter, flags)));
            return counter;
        }
        else
        {
            rdsn_assert (it2->second.second == flags, "counters with the same name %s.%s with differnt types", section_name, name);
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

void perf_counters::register_factory(perf_counter_factory factory)
{
    auto_write_lock l(_lock);
    _factory = factory;
}

} } // end namespace

