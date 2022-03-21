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
#include <cstring>
#include <sstream>
#include <openssl/md5.h>
#include <dsn/utility/strings.h>

namespace dsn {
namespace utils {

std::string get_last_component(const std::string &input, const char splitters[])
{
    int index = -1;
    const char *s = splitters;

    while (*s != 0) {
        auto pos = input.find_last_of(*s);
        if (pos != std::string::npos && (static_cast<int>(pos) > index))
            index = static_cast<int>(pos);
        s++;
    }

    if (index != -1)
        return input.substr(index + 1);
    else
        return input;
}

void split_args(const char *args,
                /*out*/ std::vector<std::string> &sargs,
                char splitter,
                bool keep_place_holder)
{
    sargs.clear();
    std::string v(args);
    uint64_t last_pos = 0;
    while (true) {
        auto pos = v.find(splitter, last_pos);
        if (pos != std::string::npos) {
            std::string s = trim_string((char *)v.substr(last_pos, pos - last_pos).c_str());
            if (!s.empty()) {
                sargs.push_back(s);
            } else if (keep_place_holder) {
                sargs.emplace_back("");
            }
            last_pos = pos + 1;
        } else {
            std::string s = trim_string((char *)v.substr(last_pos).c_str());
            if (!s.empty()) {
                sargs.push_back(s);
            } else if (keep_place_holder) {
                sargs.emplace_back("");
            }
            break;
        }
    }
}

void split_args(const char *args,
                /*out*/ std::unordered_set<std::string> &sargs,
                char splitter,
                bool keep_place_holder)
{
    std::vector<std::string> sargs_vec;
    split_args(args, sargs_vec, splitter, keep_place_holder);
    sargs.insert(sargs_vec.begin(), sargs_vec.end());
}

void split_args(const char *args, /*out*/ std::list<std::string> &sargs, char splitter)
{
    sargs.clear();

    std::string v(args);

    int lastPos = 0;
    while (true) {
        auto pos = v.find(splitter, lastPos);
        if (pos != std::string::npos) {
            std::string s = v.substr(lastPos, pos - lastPos);
            if (s.length() > 0) {
                std::string s2 = trim_string((char *)s.c_str());
                if (s2.length() > 0)
                    sargs.push_back(s2);
            }
            lastPos = static_cast<int>(pos + 1);
        } else {
            std::string s = v.substr(lastPos);
            if (s.length() > 0) {
                std::string s2 = trim_string((char *)s.c_str());
                if (s2.length() > 0)
                    sargs.push_back(s2);
            }
            break;
        }
    }
}

bool parse_kv_map(const char *args,
                  /*out*/ std::map<std::string, std::string> &kv_map,
                  char item_splitter,
                  char kv_splitter,
                  bool allow_dup_key)
{
    kv_map.clear();
    std::vector<std::string> splits;
    split_args(args, splits, item_splitter);
    for (std::string &i : splits) {
        if (i.empty())
            continue;
        size_t pos = i.find(kv_splitter);
        if (pos == std::string::npos) {
            return false;
        }
        std::string key = i.substr(0, pos);
        std::string value = i.substr(pos + 1);
        if (!allow_dup_key && kv_map.find(key) != kv_map.end()) {
            return false;
        }
        kv_map[key] = value;
    }
    return true;
}

void kv_map_to_stream(const std::map<std::string, std::string> &kv_map,
                      /*out*/ std::ostream &oss,
                      char item_splitter,
                      char kv_splitter)
{
    int i = 0;
    for (auto &kv : kv_map) {
        if (i > 0)
            oss << item_splitter;
        oss << kv.first << kv_splitter << kv.second;
        i++;
    }
}

std::string kv_map_to_string(const std::map<std::string, std::string> &kv_map,
                             char item_splitter,
                             char kv_splitter)
{
    std::ostringstream oss;
    kv_map_to_stream(kv_map, oss, item_splitter, kv_splitter);
    return oss.str();
}

std::string
replace_string(std::string subject, const std::string &search, const std::string &replace)
{
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
        subject.replace(pos, search.length(), replace);
        pos += replace.length();
    }
    return subject;
}

char *trim_string(char *s)
{
    while (*s != '\0' && (*s == ' ' || *s == '\t')) {
        s++;
    }
    char *r = s;
    s += strlen(s);
    while (s >= r && (*s == '\0' || *s == ' ' || *s == '\t' || *s == '\r' || *s == '\n')) {
        *s = '\0';
        s--;
    }
    return r;
}

std::string string_md5(const char *buffer, unsigned length)
{
    unsigned char out[MD5_DIGEST_LENGTH];
    MD5_CTX c;
    MD5_Init(&c);

    int offset = 0;
    while (offset < length) {
        int block = length - offset;
        if (block > 4096)
            block = 4096;
        MD5_Update(&c, buffer, block);
        offset += block;
        buffer += block;
    }
    MD5_Final(out, &c);

    char str[MD5_DIGEST_LENGTH * 2 + 1];
    str[MD5_DIGEST_LENGTH * 2] = 0;
    for (int n = 0; n < MD5_DIGEST_LENGTH; n++)
        sprintf(str + n + n, "%02x", out[n]);

    std::string result;
    result.assign(str);

    return result;
}
} // namespace utils
} // namespace dsn
