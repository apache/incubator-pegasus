#include <cstring>
#include <dsn/perf_counter/perf_counter.h>

static const char *ctypes[] = {
    "NUMBER", "VOLATILE_NUMBER", "RATE", "PERCENTILE", "INVALID_COUNTER"};
const char *dsn_counter_type_to_string(dsn_perf_counter_type_t t)
{
    if (t >= COUNTER_TYPE_COUNT)
        return ctypes[COUNTER_TYPE_COUNT];
    return ctypes[t];
}

dsn_perf_counter_type_t dsn_counter_type_from_string(const char *str)
{
    for (int i = 0; i < COUNTER_TYPE_COUNT; ++i) {
        if (strcmp(str, ctypes[i]) == 0)
            return (dsn_perf_counter_type_t)i;
    }
    return COUNTER_TYPE_INVALID;
}

static const char *ptypes[] = {"P50", "P90", "P95", "P99", "P999", "INVALID_PERCENTILE"};
const char *dsn_percentile_type_to_string(dsn_perf_counter_percentile_type_t t)
{
    if (t >= COUNTER_PERCENTILE_COUNT)
        return ptypes[COUNTER_PERCENTILE_COUNT];
    return ptypes[t];
}

dsn_perf_counter_percentile_type_t dsn_percentile_type_from_string(const char *str)
{
    for (int i = 0; i < COUNTER_PERCENTILE_COUNT; ++i) {
        if (strcmp(str, ptypes[i]) == 0)
            return (dsn_perf_counter_percentile_type_t)i;
    }
    return COUNTER_PERCENTILE_INVALID;
}
