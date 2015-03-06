# pragma once

# include <memory>
# include <rdsn/internal/enum_helper.h>

namespace rdsn {

enum perf_counter_type
{
    COUNTER_TYPE_NUMBER,
    COUNTER_TYPE_RATE,
    COUNTER_TYPE_NUMBER_PERCENTILES,
    COUNTER_TYPE_INVALID,
    COUNTER_TYPE_COUNT
};

ENUM_BEGIN(perf_counter_type, COUNTER_TYPE_INVALID)
    ENUM_REG(COUNTER_TYPE_NUMBER)
    ENUM_REG(COUNTER_TYPE_RATE)
    ENUM_REG(COUNTER_TYPE_NUMBER_PERCENTILES)
ENUM_END(perf_counter_type)

enum counter_percentile_type
{
    COUNTER_PERCENTILE_999,
    COUNTER_PERCENTILE_99,
    COUNTER_PERCENTILE_95,
    COUNTER_PERCENTILE_90,
    COUNTER_PERCENTILE_50,

    COUNTER_PERCENTILE_COUNT,
    COUNTER_PERCENTILE_INVALID
};

ENUM_BEGIN(counter_percentile_type, COUNTER_PERCENTILE_INVALID)
    ENUM_REG(COUNTER_PERCENTILE_999)
    ENUM_REG(COUNTER_PERCENTILE_99)
    ENUM_REG(COUNTER_PERCENTILE_95)
    ENUM_REG(COUNTER_PERCENTILE_90)
    ENUM_REG(COUNTER_PERCENTILE_50)
ENUM_END(counter_percentile_type)

class perf_counter;
typedef perf_counter* (*perf_counter_factory)(const char *section, const char *name, perf_counter_type type);

class perf_counter
{
public:
    template <typename T> static perf_counter* create(const char *section, const char *name, perf_counter_type type)
    {
        return new T(section, name, type);
    }

public:
    perf_counter(const char *section, const char *name, perf_counter_type type) {}
    virtual ~perf_counter(void) {}

    virtual void   increment() = 0;
    virtual void   decrement() = 0;
    virtual void   add(uint64_t val) = 0;
    virtual void   set(uint64_t val) = 0;
    virtual double get_value() = 0;
    virtual double get_percentile(counter_percentile_type type) = 0;
};

typedef std::shared_ptr<perf_counter> perf_counter_ptr;

} // end namespace
