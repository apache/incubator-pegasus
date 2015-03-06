# pragma once

# include <rdsn/internal/perf_counter.h>
# include <rdsn/internal/singleton.h>
# include <rdsn/internal/synchronize.h>
# include <map>

namespace rdsn { namespace utils {

class perf_counters : public rdsn::utils::singleton<perf_counters>
{
public:
    perf_counters(void);
    ~perf_counters(void);

    perf_counter_ptr get_counter(
                    const char *section, 
                    const char *name, 
                    perf_counter_type flags, 
                    bool create_if_not_exist = false
                    );

    bool remove_counter(const char* section, const char* name);

    perf_counter_ptr get_counter(
                    const char *name, 
                    perf_counter_type flags, 
                    bool create_if_not_exist = false)
    {
        return get_counter("rdsn", name, flags, create_if_not_exist);
    }

    bool remove_counter(const char* name)
    {
        return remove_counter("rdsn", name);
    }

    void register_factory(perf_counter_factory factory);

private:
    typedef std::map<std::string, std::pair<perf_counter_ptr, perf_counter_type> > same_section_counters;
    typedef std::map<std::string, same_section_counters> all_counters;

    mutable utils::rw_lock  _lock;
    all_counters         _counters;
    perf_counter_factory _factory;
};

}} // end namespace rdsn::utils
