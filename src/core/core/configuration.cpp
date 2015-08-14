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
# include <dsn/internal/configuration.h>
# include <cassert>
# include <dsn/cpp/utils.h>
# include <errno.h>
# include <iostream>

namespace dsn {


// arguments: k1=v1;k2=v2;k3=v3; ...
// e.g.,
//    port = %port%
//    timeout = %timeout%
// arguments: port=23466;timeout=1000
configuration::configuration(const char* file_name, const char* arguments)
{
    _warning = false;
    _file_name = std::string(file_name);

    FILE* fd = ::fopen(file_name, "rb");
    if (fd == nullptr) 
    {
        char cdir[FILENAME_MAX];
        getcwd_(cdir, sizeof(cdir));
        printf("Cannot open file %s in %s, err=%s", file_name, cdir, strerror(errno));
        return;
    }
    ::fseek(fd, 0, SEEK_END);
    int len = ftell(fd);
    if (len == -1 || len == 0) 
    {
        printf("Cannot get length of %s, err=%s", file_name, strerror(errno));
        ::fclose(fd);
        return;
    }

    int file_length = len;
    _file_data.resize(len + 1);
    char* fdata = (char*)_file_data.c_str();

    ::fseek(fd, 0, SEEK_SET);
    auto sz = ::fread(fdata, len, 1, fd);
    ::fclose(fd);
    if (sz != 1)
    {
        printf("Cannot read correct data of %s, err=%s", file_name, strerror(errno));
        return;
    }
    ((char*)fdata)[file_length] = '\n';


    // replace data with arguments
    if (arguments != nullptr)
    {
        std::list<std::string> argkvs;
        utils::split_args(arguments, argkvs, ';');
        for (auto& kv : argkvs)
        {
            std::list<std::string> vs;
            utils::split_args(kv.c_str(), vs, '=');
            if (vs.size() != 2)
            {
                printf("invalid configuration argument: '%s' in '%s'\n", kv.c_str(), arguments);
                return;
            }

            std::string key = std::string("%") + *vs.begin() + std::string("%");
            std::string value = *vs.rbegin();
            _file_data = utils::replace_string(_file_data, key, value);
        }
    }
    //
    // parse mapped file and build conf map
    //
    std::map<std::string, conf*>* pSection = nullptr;
    char *p, *pLine = (char*)"", *pNextLine, *pEnd, *pSectionName = nullptr, *pEqual;
    int lineno = 0;
    unsigned int indexInSection = 0;

    p = (char*)fdata;
    pEnd = p + file_length;

    while (p < pEnd) {
        //
        // get line
        //
        lineno++;
        while (*p == ' ' || *p == '\t' || *p == '\r')    p++;

        pLine = p;
        int shift = 0;
        while (*p != '\n' && p < pEnd)    
        {
            if (*p == '#' || *p == ';')
            {
                if (p != pLine && *(p-1) == '^')
                {
                    shift++;
                }
                else
                {
                    *p = '\0';
                }
            }

            if (shift > 0)
            {
                *(p-shift) = *p;
            }
            p++;
        }
        *(p-shift) = '\0';
        pNextLine = ++p;

        //
        // parse line
        //
        p = pLine;
        if (*p == '\0')    goto Next;    // skip comment line or empty line
        pEqual = strchr(p, '=');
        if (nullptr == pEqual && *p != '[') {
            goto ConfReg;
        }
        if (nullptr != pEqual && *p == '[') 
            goto err;

        //
        //    conf
        //
        if (pEqual) 
        {
ConfReg:
            if (pSection == nullptr) {
                printf("configuration section not defined");
                goto err;
            }
            if (pEqual)    *pEqual = '\0';
            char* pKey = utils::trim_string(p);
            char* pValue = pEqual ? utils::trim_string(++pEqual) : nullptr;
            if (*pKey == '\0')    
                goto err;

            if (pSection->find((const char*)pKey) != pSection->end()) 
            {
                auto it = pSection->find((const char*)pKey);

                printf("Warning: skip redefinition of option [%s] %s (line %u), already defined as [%s] %s (line %u)\n", 
                    pSectionName,
                    pKey,
                    lineno,
                    it->second->section.c_str(),
                    it->second->key.c_str(),
                    it->second->line
                    );
            }
            else
            {
                conf* cf = new conf;
                cf->section = (const char*)pSectionName;
                cf->key = pKey;
                cf->value = pValue ? pValue : "";
                cf->line = lineno; 
                cf->present = true;
                pSection->insert(std::make_pair(std::string(pKey), cf));
            }            
        }
        //
        //    section
        //
        else 
        {
            char* pRight = strchr(p, ']');
            if (nullptr == pRight)   
                goto err;
            *pRight = '\0';
            p++;
            pSectionName = utils::trim_string(p);
            if (*pSectionName == '\0')   
                goto err;

            bool old = set_warning(false);
            if (has_section((const char*)pSectionName)) {
                printf("RedefInition of section %s\n", pSectionName);
                set_warning(old);
                goto err;
            }
            set_warning(old);

            std::map<std::string, conf*> sm;
            auto it = _configs.insert(config_map::value_type(std::string(pSectionName), sm));
            assert (it.second);
            pSection = &it.first->second;
            indexInSection = 0;
        }

        //
        // iterate nextline
        //
Next:
        p = pNextLine;
    }
    return;
    
err:
    printf("Unexpected configure in %s(line %d): %s\n", file_name, lineno, pLine);
    exit(-2);
}

configuration::~configuration(void)
{
}

void configuration::get_all_sections(std::vector<std::string>& sections)
{
    for (auto it = _configs.begin(); it != _configs.end(); it++)
    {
        sections.push_back(it->first);
    }
}

void configuration::get_all_keys(const char* section, std::vector<const char*>& keys)
{
    auto it = _configs.find(section);
    if (it != _configs.end())
    {
        for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++)
        {
            keys.push_back(it2->first.c_str());
        }
    }
}

bool configuration::get_string_value_internal(const char* section, const char* key, const char* default_value, const char** ov, const char* dsptr)
{
    _lock.lock();

    std::map<std::string, conf*> *ps = nullptr;
    auto it = _configs.find(section);
    if (it != _configs.end())
    {
        ps = &it->second;
        auto it2 = it->second.find(key);
        if (it2 != it->second.end())
        {
            if (it2->second->present)
            {
                if (it2->second->dsptr.length() == 0)
                it2->second->dsptr = dsptr;

                *ov = it2->second->value.c_str();

                _lock.unlock();
                return true;
            }
            else
            {
                _lock.unlock();
                return false;
            }
        }
    }
    
    if (ps == nullptr)
    {
        std::map<std::string, conf*> sm;
        auto it = _configs.insert(config_map::value_type(std::string(section), sm));
        assert(it.second);
        ps = &it.first->second;
    }

    conf* cf = new conf();
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

const char* configuration::get_string_value(const char* section, const char* key, const char* default_value, const char* dsptr)
{
    const char* ov;
    if (!get_string_value_internal(section, key, default_value, &ov, dsptr))
    {
        if (_warning)
        {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%s'\n",
                section,
                key,
                default_value
                );
        }
    }
    return ov;
}

std::list<std::string> configuration::get_string_value_list(const char* section, const char* key, char splitter, const char* dsptr)
{
    const char* ov;
    if (!get_string_value_internal(section, key, "", &ov, dsptr))
    {
        if (_warning)
        {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%s'\n",
                section,
                key,
                ""
                );
        }
    }

    std::list<std::string> vs;
    utils::split_args(ov, vs, splitter);

    for (auto& v : vs)
    {
        v = std::string(utils::trim_string((char*)v.c_str()));
    }
    return vs;
}

void configuration::dump(std::ostream& os)
{
    _lock.lock();

    for (auto& s : _configs)
    {
        os << "[" << s.first << "]" << std::endl;

        for (auto& kv : s.second)
        {
            os << "; " << kv.second->dsptr << std::endl;
            os << kv.first << " = " << kv.second->value << std::endl << std::endl;
        }

        os << std::endl;
    }

    _lock.unlock();
}

void configuration::register_config_change_notification(config_file_change_notifier notifier)
{
    dassert (false, "not implemented");
}

bool configuration::has_section(const char* section)
{
    auto it = _configs.find(section);
    bool r = (it != _configs.end());
    if (!r && _warning)
    {
        printf("WARNING: configuration section '[%s]' is not defined, using default settings\n", section);
    }
    return r;
}

bool configuration::has_key(const char* section, const char* key)
{
    auto it = _configs.find(section);
    if (it != _configs.end())
    {
        auto it2 = it->second.find(key);
        return (it2 != it->second.end());
    }
    return false;
}

}
