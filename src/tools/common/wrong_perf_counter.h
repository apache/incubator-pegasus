#pragma once

# include <rdsn/tool_api.h>

namespace rdsn {
    namespace tools {

        class wrong_perf_counter : public perf_counter
        {
        public:
            wrong_perf_counter(const char *section, const char *name, perf_counter_type type)
                : perf_counter(section, name, type)
            {}
            ~wrong_perf_counter(void) {}

            virtual void   increment() { _val++; }
            virtual void   decrement() { _val--; }
            virtual void   add(uint64_t val) { _val += val; }
            virtual void   set(uint64_t val) { _val = val; } // sample
            virtual double get_value() { return (double)_val.load(); }
            virtual double get_percentile(counter_percentile_type type) { return 0.0; }

        private:
            std::atomic<uint64_t> _val;
        };

    }
}

