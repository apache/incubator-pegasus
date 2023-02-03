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

#include <algorithm>
#include <cstring>
#include <sstream>
#include <strings.h>
#include <utility>

#include <openssl/md5.h>

#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/strings.h"

namespace dsn {
namespace utils {

#define CHECK_NULL_PTR(lhs, rhs)                                                                   \
    do {                                                                                           \
        if (lhs == nullptr) {                                                                      \
            return rhs == nullptr;                                                                 \
        }                                                                                          \
        if (rhs == nullptr) {                                                                      \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

bool equals(const char *lhs, const char *rhs)
{
    CHECK_NULL_PTR(lhs, rhs);
    return std::strcmp(lhs, rhs) == 0;
}

bool equals(const char *lhs, const char *rhs, size_t n)
{
    CHECK_NULL_PTR(lhs, rhs);
    return std::strncmp(lhs, rhs, n) == 0;
}

bool iequals(const char *lhs, const char *rhs)
{
    CHECK_NULL_PTR(lhs, rhs);
    return ::strcasecmp(lhs, rhs) == 0;
}

bool iequals(const char *lhs, const char *rhs, size_t n)
{
    CHECK_NULL_PTR(lhs, rhs);
    return ::strncasecmp(lhs, rhs, n) == 0;
}

bool iequals(const std::string &lhs, const char *rhs)
{
    if (rhs == nullptr) {
        return false;
    }
    return ::strcasecmp(lhs.c_str(), rhs) == 0;
}

bool iequals(const std::string &lhs, const char *rhs, size_t n)
{
    if (rhs == nullptr) {
        return false;
    }
    return ::strncasecmp(lhs.c_str(), rhs, n) == 0;
}

bool iequals(const char *lhs, const std::string &rhs)
{
    if (lhs == nullptr) {
        return false;
    }
    return ::strcasecmp(lhs, rhs.c_str()) == 0;
}

bool iequals(const char *lhs, const std::string &rhs, size_t n)
{
    if (lhs == nullptr) {
        return false;
    }
    return ::strncasecmp(lhs, rhs.c_str(), n) == 0;
}

bool mequals(const void *lhs, const void *rhs, size_t n)
{
    CHECK_NULL_PTR(lhs, rhs);
    return std::memcmp(lhs, rhs, n) == 0;
}

#undef CHECK_NULL_PTR

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

namespace {

// The states while scan each split.
enum class split_args_state : int
{
    kSplitBeginning,     // The initial state while starting to scan a split
    kSplitLeadingSpaces, // While meeting leading spaces, if any
    kSplitToken,         // While running into token (after leading spaces)
};

const std::string kLeadingSpaces = " \t";
const std::string kTrailingSpaces = " \t\r\n";

inline bool is_leading_space(char ch)
{
    return std::any_of(
        kLeadingSpaces.begin(), kLeadingSpaces.end(), [ch](char space) { return ch == space; });
}

inline bool is_trailing_space(char ch)
{
    return std::any_of(
        kTrailingSpaces.begin(), kTrailingSpaces.end(), [ch](char space) { return ch == space; });
}

// Skip trailing spaces and find the end of token.
const char *find_token_end(const char *token_begin, const char *end)
{
    CHECK(token_begin < end, "");

    for (; end > token_begin && is_trailing_space(*(end - 1)); --end) {
    }
    return end;
}

// Append new element to sequence containers, such as std::vector and std::list.
struct SequenceInserter
{
    // The new element is constructed through variadic template and appended at the end
    // of the sequence container.
    template <typename SequenceContainer, typename... Args>
    void emplace(SequenceContainer &container, Args &&... args) const
    {
        container.emplace_back(std::forward<Args>(args)...);
    }
};

// Insert new element to associative containers, such as std::unordered_set and std::set.
struct AssociativeInserter
{
    // The new element is constructed through variadic template and inserted into the associative
    // container.
    template <typename AssociativeContainer, typename... Args>
    void emplace(AssociativeContainer &container, Args &&... args) const
    {
        container.emplace(std::forward<Args>(args)...);
    }
};

// Split the `input` string by the only character `separator` into tokens. Leading and trailing
// spaces of each token will be stripped. Once the token is empty, or become empty after
// stripping, an empty string will be added into `output` if `keep_place_holder` is enabled.
//
// `inserter` provides the only interface for all types of containers. By `inserter`, all tokens
// will be collected to each type of container: for sequence containers, such as std::vector and
// std::list, tokens will be "appended"; for associative containers, such as std::unordered_set
// and std::set, tokens will be "inserted".
template <typename Inserter, typename Container>
void split(const char *input,
           char separator,
           bool keep_place_holder,
           const Inserter &inserter,
           Container &output)
{
    CHECK_NOTNULL(input, "");

    output.clear();

    auto state = split_args_state::kSplitBeginning;
    const char *token_begin = nullptr;
    for (auto p = input;; ++p) {
        if (*p == separator || *p == '\0') {
            switch (state) {
            case split_args_state::kSplitBeginning:
            case split_args_state::kSplitLeadingSpaces:
                if (keep_place_holder) {
                    // Will add an empty string as output, since all characters
                    // in this split are spaces.
                    inserter.emplace(output);
                }
                break;
            case split_args_state::kSplitToken: {
                auto token_end = find_token_end(token_begin, p);
                CHECK(token_begin <= token_end, "");
                if (token_begin == token_end && !keep_place_holder) {
                    // Current token is empty, and place holder is not needed.
                    break;
                }
                inserter.emplace(output, token_begin, token_end);
            } break;
            default:
                break;
            }

            if (*p == '\0') {
                // The whole string has been scanned, just break from the loop.
                break;
            }

            // Current token has been scanned, continue next split.
            state = split_args_state::kSplitBeginning;
            continue;
        }

        // Current scanned character is not `separator`.
        switch (state) {
        case split_args_state::kSplitBeginning:
            if (is_leading_space(*p)) {
                state = split_args_state::kSplitLeadingSpaces;
            } else {
                state = split_args_state::kSplitToken;
                token_begin = p;
            }
            break;
        case split_args_state::kSplitLeadingSpaces:
            if (!is_leading_space(*p)) {
                // Any character that is not leading space will be considered as
                // the beginning of a token. Whether all of the scanned characters
                // belong to the token will be decided while the `separator` is
                // found.
                state = split_args_state::kSplitToken;
                token_begin = p;
            }
            break;
        default:
            break;
        }
    }
}

template <typename Container>
inline void split_to_sequence_container(const char *input,
                                        char separator,
                                        bool keep_place_holder,
                                        Container &output)
{
    split(input, separator, keep_place_holder, SequenceInserter(), output);
}

template <typename Container>
inline void split_to_associative_container(const char *input,
                                           char separator,
                                           bool keep_place_holder,
                                           Container &output)
{
    split(input, separator, keep_place_holder, AssociativeInserter(), output);
}

} // anonymous namespace

void split_args(const char *input,
                std::vector<std::string> &output,
                char separator,
                bool keep_place_holder)
{
    split_to_sequence_container(input, separator, keep_place_holder, output);
}

void split_args(const char *input,
                std::list<std::string> &output,
                char separator,
                bool keep_place_holder)
{
    split_to_sequence_container(input, separator, keep_place_holder, output);
}

void split_args(const char *input,
                std::unordered_set<std::string> &output,
                char separator,
                bool keep_place_holder)
{
    split_to_associative_container(input, separator, keep_place_holder, output);
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
    while (*s != '\0' && is_leading_space(*s)) {
        s++;
    }
    char *r = s;
    s += strlen(s);
    while (s >= r && (*s == '\0' || is_trailing_space(*s))) {
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

std::string find_string_prefix(const std::string &input, char separator)
{
    auto current = input.find(separator);
    if (current == 0 || current == std::string::npos) {
        return std::string();
    }
    return input.substr(0, current);
}
} // namespace utils
} // namespace dsn
