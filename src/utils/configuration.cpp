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

#include <fmt/core.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <utility>

#include "utils/configuration.h"
#include "utils/env.h"
#include "utils/strings.h"

namespace dsn {

configuration::configuration() { _warning = false; }

configuration::~configuration()
{
    for (auto &section_kv : _configs) {
        auto &section = section_kv.second;
        for (auto &kv : section) {
            delete kv.second;
        }
    }
    _configs.clear();
}

// arguments: k1=v1;k2=v2;k3=v3; ...
// e.g.,
//    port = %port%
//    timeout = %timeout%
// arguments: port=23466;timeout=1000 or arguments: ports=23466,timout=1000
bool configuration::load(const char *file_name, const char *arguments)
{
    _file_name = std::string(file_name);

    auto s = rocksdb::ReadFileToString(
        dsn::utils::PegasusEnv(dsn::utils::FileDataType::kNonSensitive), _file_name, &_file_data);
    if (!s.ok()) {
        fmt::print(stderr, "ERROR: read file '{}' failed, err = {}\n", _file_name, s.ToString());
        return false;
    }

    if (_file_data.empty()) {
        fmt::print(stderr, "ERROR: file '{}' is empty\n", _file_name);
        return false;
    }

    // replace data with arguments
    if (arguments != nullptr) {
        std::string str_arguments(arguments);
        std::replace(str_arguments.begin(), str_arguments.end(), ',', ';');
        std::list<std::string> argkvs;
        utils::split_args(str_arguments.c_str(), argkvs, ';');
        for (auto &kv : argkvs) {
            std::list<std::string> vs;
            utils::split_args(kv.c_str(), vs, '=');
            if (vs.size() != 2) {
                printf(
                    "ERROR: invalid configuration argument: '%s' in '%s'\n", kv.c_str(), arguments);
                return false;
            }

            std::string key = std::string("%") + *vs.begin() + std::string("%");
            std::string value = *vs.rbegin();
            _file_data = utils::replace_string(_file_data, key, value);
        }
    }

    //
    // parse mapped file and build conf map
    //
    std::map<std::string, conf *> *pSection = nullptr;
    char *p, *pLine = (char *)"", *pNextLine, *pEnd, *pSectionName = nullptr, *pEqual;
    int lineno = 0;

    // ATTENTION: arguments replace_string() may cause _file_data changed,
    // so set `p' and `pEnd' carefully.
    p = (char *)_file_data.c_str();
    pEnd = p + _file_data.size();

    while (p < pEnd) {
        //
        // get line
        //
        lineno++;
        while (*p == ' ' || *p == '\t' || *p == '\r')
            p++;

        pLine = p;
        int shift = 0;
        while (*p != '\n' && p < pEnd) {
            if (*p == '#' || *p == ';') {
                if (p != pLine && *(p - 1) == '^') {
                    shift++;
                } else {
                    *p = '\0';
                }
            }

            if (shift > 0) {
                *(p - shift) = *p;
            }
            p++;
        }
        *(p - shift) = '\0';
        pNextLine = ++p;

        //
        // parse line
        //
        p = pLine;
        if (*p == '\0')
            goto Next; // skip comment line or empty line
        pEqual = strchr(p, '=');
        if (nullptr == pEqual && *p != '[') {
            goto ConfReg;
        }
        if (nullptr != pEqual && *p == '[')
            goto err;

        //
        //    conf
        //
        if (pEqual) {
        ConfReg:
            if (pSection == nullptr) {
                printf("ERROR: configuration section not defined\n");
                goto err;
            }
            if (pEqual)
                *pEqual = '\0';
            char *pKey = utils::trim_string(p);
            char *pValue = pEqual ? utils::trim_string(++pEqual) : nullptr;
            if (*pKey == '\0')
                goto err;

            if (pSection->find((const char *)pKey) != pSection->end()) {
                auto it = pSection->find((const char *)pKey);

                printf("WARNING: skip redefinition of option [%s] %s (line %u), already defined as "
                       "[%s] %s (line %u)\n",
                       pSectionName,
                       pKey,
                       lineno,
                       it->second->section.c_str(),
                       it->second->key.c_str(),
                       it->second->line);
            } else {
                conf *cf = new conf;
                cf->section = (const char *)pSectionName;
                cf->key = pKey;
                cf->line = lineno;
                cf->present = true;

                if (pValue) {
                    // if argument is not provided
                    if (strlen(pValue) > 2 && *pValue == '%' && pValue[strlen(pValue) - 1] == '%')
                        cf->value = "";
                    else
                        cf->value = pValue;
                } else {
                    cf->value = "";
                }

                pSection->insert(std::make_pair(std::string(pKey), cf));
            }
        }
        //
        //    section
        //
        else {
            char *pRight = strchr(p, ']');
            if (nullptr == pRight)
                goto err;
            *pRight = '\0';
            p++;
            pSectionName = utils::trim_string(p);
            if (*pSectionName == '\0')
                goto err;

            bool old = set_warning(false);
            if (has_section((const char *)pSectionName)) {
                printf("ERROR: configuration section '[%s]' is redefined\n", pSectionName);
                set_warning(old);
                goto err;
            }
            set_warning(old);

            std::map<std::string, conf *> sm;
            auto it = _configs.insert(config_map::value_type(std::string(pSectionName), sm));
            assert(it.second);
            pSection = &it.first->second;
        }

    //
    // iterate nextline
    //
    Next:
        p = pNextLine;
    }
    return true;

err:
    printf("ERROR: unexpected configuration in %s(line %d): %s\n", file_name, lineno, pLine);
    return false;
}

void configuration::get_all_section_ptrs(std::vector<const char *> &sections)
{
    sections.clear();
    for (auto it = _configs.begin(); it != _configs.end(); ++it) {
        sections.push_back(it->first.c_str());
    }
}

void configuration::get_all_sections(std::vector<std::string> &sections)
{
    sections.clear();
    for (auto it = _configs.begin(); it != _configs.end(); ++it) {
        sections.push_back(it->first);
    }
}

void configuration::get_all_keys(const char *section, std::vector<const char *> &keys)
{
    std::multimap<int, const char *> ordered_keys;
    keys.clear();
    auto it = _configs.find(section);
    if (it != _configs.end()) {
        for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
            ordered_keys.emplace(it2->second->line, it2->first.c_str());
        }
    }

    for (auto &k : ordered_keys) {
        keys.push_back(k.second);
    }
}

bool configuration::get_string_value_internal(const char *section,
                                              const char *key,
                                              const char *default_value,
                                              const char **ov,
                                              const char *dsptr)
{
    _lock.lock();

    std::map<std::string, conf *> *ps = nullptr;
    auto it = _configs.find(section);
    if (it != _configs.end()) {
        ps = &it->second;
        auto it2 = it->second.find(key);
        if (it2 != it->second.end()) {
            if (!it2->second->present) {
                if (it2->second->value != default_value) {
                    printf("ERROR: configuration default value is different for '[%s] %s': %s <--> "
                           "%s\n",
                           section,
                           key,
                           it2->second->value.c_str(),
                           default_value);
                    ::abort();
                }
            }

            if (it2->second->dsptr.length() == 0)
                it2->second->dsptr = dsptr;

            *ov = it2->second->value.c_str();
            bool ret = it2->second->present ? true : false;

            _lock.unlock();
            return ret;
        }
    }

    if (ps == nullptr) {
        std::map<std::string, conf *> sm;
        auto it = _configs.insert(config_map::value_type(std::string(section), sm));
        assert(it.second);
        ps = &it.first->second;
    }

    conf *cf = new conf();
    cf->dsptr = dsptr;
    cf->key = key;
    cf->value = default_value;
    cf->line = 0;
    cf->present = false;
    cf->section = section;
    ps->insert(std::make_pair(cf->key, cf));

    *ov = cf->value.c_str();

    _lock.unlock();
    return false;
}

const char *configuration::get_string_value(const char *section,
                                            const char *key,
                                            const char *default_value,
                                            const char *dsptr)
{
    const char *ov;
    if (!get_string_value_internal(section, key, default_value, &ov, dsptr)) {
        if (_warning) {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%s'\n",
                   section,
                   key,
                   default_value);
        }
    }
    return ov;
}

std::list<std::string> configuration::get_string_value_list(const char *section,
                                                            const char *key,
                                                            char splitter,
                                                            const char *dsptr)
{
    const char *ov;
    if (!get_string_value_internal(section, key, "", &ov, dsptr)) {
        if (_warning) {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%s'\n",
                   section,
                   key,
                   "");
        }
    }

    std::list<std::string> vs;
    utils::split_args(ov, vs, splitter);

    for (auto &v : vs) {
        v = std::string(utils::trim_string((char *)v.c_str()));
    }
    return vs;
}

void configuration::dump(std::ostream &os)
{
    _lock.lock();

    for (auto &s : _configs) {
        os << "[" << s.first << "]" << std::endl;

        std::multimap<int, conf *> ordered_entities;
        for (auto &kv : s.second) {
            ordered_entities.emplace(kv.second->line, kv.second);
        }

        for (auto &kv : ordered_entities) {
            os << "; " << kv.second->dsptr << std::endl;
            os << kv.second->key << " = " << kv.second->value << std::endl << std::endl;
        }

        os << std::endl;
    }

    _lock.unlock();
}

void configuration::set(const char *section, const char *key, const char *value, const char *dsptr)
{
    std::map<std::string, conf *> *psection;

    _lock.lock();

    auto it = _configs.find(section);
    if (it != _configs.end()) {
        psection = &it->second;
    } else {
        std::map<std::string, conf *> s;
        psection = &_configs.insert(config_map::value_type(section, s)).first->second;
    }

    auto it2 = psection->find(key);
    if (it2 == psection->end()) {
        conf *cf = new conf();
        cf->dsptr = dsptr;
        cf->key = key;
        cf->value = value;
        cf->line = 0;
        cf->present = true;
        cf->section = section;
        psection->insert(std::make_pair(cf->key, cf));
    } else {
        it2->second->value = value;
    }

    _lock.unlock();
}

bool configuration::has_section(const char *section)
{
    auto it = _configs.find(section);
    bool r = (it != _configs.end());
    if (!r && _warning) {
        printf("WARNING: configuration section '[%s]' is not defined, using default settings\n",
               section);
    }
    return r;
}

bool configuration::has_key(const char *section, const char *key)
{
    auto it = _configs.find(section);
    if (it != _configs.end()) {
        auto it2 = it->second.find(key);
        return (it2 != it->second.end());
    }
    return false;
}
}
