#include <fstream>
#include <dsn/utility/config_api.h>
#include <dsn/utility/configuration.h>

dsn::configuration g_config;

bool dsn_config_load(const char *file, const char *arguments)
{
    return g_config.load(file, arguments);
}

void dsn_config_dump(std::ostream &os) { g_config.dump(os); }

const char *dsn_config_get_value_string(const char *section,
                                        const char *key,
                                        const char *default_value,
                                        const char *dsptr)
{
    return g_config.get_string_value(section, key, default_value, dsptr);
}

bool dsn_config_get_value_bool(const char *section,
                               const char *key,
                               bool default_value,
                               const char *dsptr)
{
    return g_config.get_value<bool>(section, key, default_value, dsptr);
}

uint64_t dsn_config_get_value_uint64(const char *section,
                                     const char *key,
                                     uint64_t default_value,
                                     const char *dsptr)
{
    return g_config.get_value<uint64_t>(section, key, default_value, dsptr);
}

int64_t dsn_config_get_value_int64(const char *section,
                                   const char *key,
                                   int64_t default_value,
                                   const char *dsptr)
{
    return g_config.get_value<int64_t>(section, key, default_value, dsptr);
}

double dsn_config_get_value_double(const char *section,
                                   const char *key,
                                   double default_value,
                                   const char *dsptr)
{
    return g_config.get_value<double>(section, key, default_value, dsptr);
}

void dsn_config_get_all_sections(/*out*/ std::vector<std::string> &sections)
{
    g_config.get_all_sections(sections);
}

void dsn_config_get_all_sections(/*out*/ std::vector<const char *> &sections)
{
    g_config.get_all_section_ptrs(sections);
}

void dsn_config_get_all_keys(const char *section, std::vector<std::string> &keys)
{
    std::vector<const char *> key_ptrs;
    g_config.get_all_keys(section, key_ptrs);
    for (const char *p : key_ptrs)
        keys.emplace_back(std::string(p));
}

void dsn_config_get_all_keys(const char *section, /*out*/ std::vector<const char *> &keys)
{
    g_config.get_all_keys(section, keys);
}

void dsn_config_set(const char *section, const char *key, const char *value, const char *dsptr)
{
    g_config.set(section, key, value, dsptr);
}
