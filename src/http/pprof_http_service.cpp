// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifdef DSN_ENABLE_GPERF

#include "pprof_http_service.h"

#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <sstream>

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>

#include "runtime/api_layer1.h"
#include "utils/defer.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/string_conv.h"
#include "utils/string_splitter.h"
#include "utils/strings.h"
#include "utils/timer.h"

namespace dsn {

//                            //
// == ip:port/pprof/symbol == //
//                            //

typedef std::map<uintptr_t, std::string> symbol_map_t;
static symbol_map_t symbol_map;
static pthread_once_t s_load_symbolmap_once = PTHREAD_ONCE_INIT;

struct lib_info
{
    uintptr_t start_addr;
    uintptr_t end_addr;
    size_t offset;
    std::string path;
};

static bool has_ext(const std::string &name, const std::string &ext)
{
    size_t index = name.find(ext);
    if (index == std::string::npos) {
        return false;
    }
    return (index + ext.size() == name.size() || name[index + ext.size()] == '.');
}

static int extract_symbols_from_binary(std::map<uintptr_t, std::string> &addr_map,
                                       const lib_info &lib_info)
{
    timer tm;
    tm.start();
    std::string cmd = "nm -C -p ";
    cmd.append(lib_info.path);
    std::stringstream ss;
    LOG_INFO("executing `{}`", cmd);
    const int rc = utils::pipe_execute(cmd.c_str(), ss);
    if (rc < 0) {
        LOG_ERROR("fail to popen `{}`", cmd);
        return -1;
    }
    std::string line;
    while (std::getline(ss, line)) {
        string_splitter sp(line.c_str(), ' ');
        if (sp == NULL) {
            continue;
        }
        char *endptr = NULL;
        uintptr_t addr = strtoull(sp.field(), &endptr, 16);
        if (*endptr != ' ') {
            continue;
        }
        if (addr < lib_info.start_addr) {
            addr = addr + lib_info.start_addr - lib_info.offset;
        }
        if (addr >= lib_info.end_addr) {
            continue;
        }
        ++sp;
        if (sp == NULL) {
            continue;
        }
        if (sp.length() != 1UL) {
            continue;
        }
        // const char c = *sp.field();

        ++sp;
        if (sp == NULL) {
            continue;
        }
        const char *name_begin = sp.field();
        if (utils::equals(name_begin, "typeinfo ", 9) || utils::equals(name_begin, "VTT ", 4) ||
            utils::equals(name_begin, "vtable ", 7) || utils::equals(name_begin, "global ", 7) ||
            utils::equals(name_begin, "guard ", 6)) {
            addr_map[addr] = std::string();
            continue;
        }

        const char *name_end = sp.field();
        bool stop = false;
        char last_char = '\0';
        while (1) {
            switch (*name_end) {
            case 0:
            case '\r':
            case '\n':
                stop = true;
                break;
            case '(':
            case '<':
                // \(.*\w\)[(<]...    -> \1
                // foo(..)            -> foo
                // foo<...>(...)      -> foo
                // a::b::foo(...)     -> a::b::foo
                // a::(b)::foo(...)   -> a::(b)::foo
                if (isalpha(last_char) || isdigit(last_char) || last_char == '_') {
                    stop = true;
                }
            default:
                break;
            }
            if (stop) {
                break;
            }
            last_char = *name_end++;
        }
        // If address conflicts, choose a shorter name (not necessarily to be
        // T type in nm). This works fine because aliases often have more
        // prefixes.
        const size_t name_len = name_end - name_begin;
        auto it = addr_map.find(addr);
        if (it != addr_map.end()) {
            if (name_len < it->second.size()) {
                it->second.assign(name_begin, name_len);
            }
        } else {
            addr_map[addr] = std::string(name_begin, name_len);
        }
    }
    if (addr_map.find(lib_info.end_addr) == addr_map.end()) {
        addr_map[lib_info.end_addr] = std::string();
    }
    tm.stop();
    LOG_INFO("Loaded {} in {}ms", lib_info.path, tm.m_elapsed().count());
    return 0;
}

static void load_symbols()
{
    timer tm;
    tm.start();
    auto fp = fopen("/proc/self/maps", "r");
    if (fp == nullptr) {
        return;
    }
    auto cleanup = defer([fp]() { fclose(fp); });

    char *line = nullptr;
    size_t line_len = 0;
    ssize_t nr = 0;
    while ((nr = getline(&line, &line_len, fp)) != -1) {
        string_splitter sp(line, line + nr, ' ');
        if (sp == NULL) {
            continue;
        }
        char *endptr;
        uintptr_t start_addr = strtoull(sp.field(), &endptr, 16);
        if (*endptr != '-') {
            continue;
        }
        ++endptr;
        uintptr_t end_addr = strtoull(endptr, &endptr, 16);
        if (*endptr != ' ') {
            continue;
        }
        ++sp;
        // ..x. must be executable
        if (sp == NULL || sp.length() != 4 || sp.field()[2] != 'x') {
            continue;
        }
        ++sp;
        if (sp == NULL) {
            continue;
        }
        size_t offset = strtoull(sp.field(), &endptr, 16);
        if (*endptr != ' ') {
            continue;
        }
        // skip $4~$5
        for (int i = 0; i < 3; ++i) {
            ++sp;
        }
        if (sp == NULL) {
            continue;
        }
        size_t n = sp.length();
        if (sp.field()[n - 1] == '\n') {
            --n;
        }
        std::string path(sp.field(), n);

        if (!has_ext(path, ".so") && !has_ext(path, ".dll") && !has_ext(path, ".dylib") &&
            !has_ext(path, ".bundle")) {
            continue;
        }
        lib_info info;
        info.start_addr = start_addr;
        info.end_addr = end_addr;
        info.offset = offset;
        info.path = path;
        extract_symbols_from_binary(symbol_map, info);
    }
    free(line);

    lib_info info;
    info.start_addr = 0;
    info.end_addr = std::numeric_limits<uintptr_t>::max();
    info.offset = 0;
    info.path = program_invocation_name;
    extract_symbols_from_binary(symbol_map, info);

    timer tm2;
    tm2.start();
    size_t num_removed = 0;
    bool last_is_empty = false;
    for (auto it = symbol_map.begin(); it != symbol_map.end();) {
        if (it->second.empty()) {
            if (last_is_empty) {
                symbol_map.erase(it++);
                ++num_removed;
            } else {
                ++it;
            }
            last_is_empty = true;
        } else {
            ++it;
        }
    }
    tm2.stop();
    if (num_removed) {
        LOG_INFO("Removed {} entries in {}ms", num_removed, tm2.m_elapsed().count());
    }

    tm.stop();
    LOG_INFO("Loaded all symbols in {}ms", tm.m_elapsed().count());
}

static void find_symbols(std::string *out, std::vector<uintptr_t> &addr_list)
{
    char buf[32];
    for (size_t i = 0; i < addr_list.size(); ++i) {
        int len = snprintf(buf, sizeof(buf), "0x%08lx\t", addr_list[i]);
        out->append(buf, static_cast<size_t>(len));
        symbol_map_t::const_iterator it = symbol_map.lower_bound(addr_list[i]);
        if (it == symbol_map.end() || it->first != addr_list[i]) {
            if (it != symbol_map.begin()) {
                --it;
            } else {
                len = snprintf(buf, sizeof(buf), "0x%08lx\n", addr_list[i]);
                out->append(buf, static_cast<size_t>(len));
                continue;
            }
        }
        if (it->second.empty()) {
            len = snprintf(buf, sizeof(buf), "0x%08lx\n", addr_list[i]);
            out->append(buf, static_cast<size_t>(len));
        } else {
            out->append(it->second);
            out->push_back('\n');
        }
    }
}

void pprof_http_service::symbol_handler(const http_request &req, http_response &resp)
{
    // Load /proc/self/maps
    pthread_once(&s_load_symbolmap_once, load_symbols);

    if (req.method != http_method::HTTP_METHOD_POST) {
        char buf[64];
        snprintf(buf, sizeof(buf), "num_symbols: %lu\n", symbol_map.size());
        resp.body = buf;
        return;
    }

    // addr_str is addresses separated by +
    std::string addr_str = req.body.to_string();
    // May be quoted
    const char *addr_cstr = addr_str.data();
    if (*addr_cstr == '\'' || *addr_cstr == '"') {
        ++addr_cstr;
    }
    std::vector<uintptr_t> addr_list;
    addr_list.reserve(32);
    string_splitter sp(addr_cstr, '+');
    for (; sp != NULL; ++sp) {
        char *endptr;
        uintptr_t addr = strtoull(sp.field(), &endptr, 16);
        addr_list.push_back(addr);
    }
    find_symbols(&resp.body, addr_list);
}

//                          //
// == ip:port/pprof/heap == //
//                          //
void pprof_http_service::heap_handler(const http_request &req, http_response &resp)
{
    bool in_pprof = false;
    if (!_in_pprof_action.compare_exchange_strong(in_pprof, true)) {
        LOG_WARNING("node is already exectuting pprof action, please wait and retry");
        resp.status_code = http_status_code::internal_server_error;
        return;
    }

    const std::string SECOND = "seconds";
    const uint32_t kDefaultSecond = 10;

    // get seconds from query params, default value is `kDefaultSecond`
    uint32_t seconds = kDefaultSecond;
    const auto iter = req.query_args.find(SECOND);
    if (iter != req.query_args.end()) {
        const auto seconds_str = iter->second;
        dsn::internal::buf2unsigned(seconds_str, seconds);
    }

    std::stringstream profile_name_prefix;
    profile_name_prefix << "heap_profile." << getpid() << "." << dsn_now_ns();

    HeapProfilerStart(profile_name_prefix.str().c_str());
    sleep(seconds);
    const char *profile = GetHeapProfile();
    HeapProfilerStop();

    resp.status_code = http_status_code::ok;
    resp.body = profile;
    delete profile;

    _in_pprof_action.store(false);
}

//                             //
// == ip:port/pprof/cmdline == //
//                             //

// Read command line of this program. If `with_args' is true, args are
// included and separated with spaces.
// Returns length of the command line on success, -1 otherwise.
// NOTE: `buf' does not end with zero.
ssize_t read_command_line(char *buf, size_t len, bool with_args)
{
    auto fd = open("/proc/self/cmdline", O_RDONLY);
    if (fd < 0) {
        LOG_ERROR("Fail to open /proc/self/cmdline");
        return -1;
    }
    auto cleanup = defer([fd]() { close(fd); });
    ssize_t nr = read(fd, buf, len);
    if (nr <= 0) {
        LOG_ERROR("Fail to read /proc/self/cmdline");
        return -1;
    }

    if (with_args) {
        if ((size_t)nr == len) {
            LOG_ERROR("buf is not big enough");
            return -1;
        }
        for (ssize_t i = 0; i < nr; ++i) {
            if (buf[i] == '\0') {
                buf[i] = '\n';
            }
        }
        return nr;
    } else {
        for (ssize_t i = 0; i < nr; ++i) {
            // The command in macos is separated with space and ended with '\n'
            if (buf[i] == '\0' || buf[i] == '\n' || buf[i] == ' ') {
                return i;
            }
        }
        if ((size_t)nr == len) {
            LOG_INFO("buf is not big enough");
            return -1;
        }
        return nr;
    }
}

void pprof_http_service::cmdline_handler(const http_request &req, http_response &resp)
{
    char buf[1024]; // should be enough?
    const ssize_t nr = read_command_line(buf, sizeof(buf), true);
    if (nr < 0) {
        return;
    }
    resp.body = buf;
}

//                             //
// == ip:port/pprof/growth == //
//                             //

void pprof_http_service::growth_handler(const http_request &req, http_response &resp)
{
    bool in_pprof = false;
    if (!_in_pprof_action.compare_exchange_strong(in_pprof, true)) {
        LOG_WARNING("node is already exectuting pprof action, please wait and retry");
        resp.status_code = http_status_code::internal_server_error;
        return;
    }

    MallocExtension *malloc_ext = MallocExtension::instance();
    LOG_INFO("received requests for growth profile");
    malloc_ext->GetHeapGrowthStacks(&resp.body);

    _in_pprof_action.store(false);
}

//                             //
// == ip:port/pprof/profile == //
//                             //
static bool get_cpu_profile(std::string &result, useconds_t seconds)
{
    const char *file_name = "cpu.prof";

    ProfilerStart(file_name);
    usleep(seconds);
    ProfilerStop();

    std::ifstream in(file_name);
    if (!in.is_open()) {
        result = "No profile file";
        return false;
    }
    std::ostringstream content;
    content << in.rdbuf();
    result = content.str();
    in.close();
    if (remove(file_name) != 0) {
        result = "Failed to remove temporary profile file";
        return false;
    }
    return true;
}

void pprof_http_service::profile_handler(const http_request &req, http_response &resp)
{
    bool in_pprof = false;
    if (!_in_pprof_action.compare_exchange_strong(in_pprof, true)) {
        LOG_WARNING("node is already exectuting pprof action, please wait and retry");
        resp.status_code = http_status_code::internal_server_error;
        return;
    }

    useconds_t seconds = 60000000;

    std::string req_url = req.full_url.to_string();
    size_t len = req.full_url.length();
    string_splitter url_sp(req_url.data(), req_url.data() + len, '?');
    if (url_sp != NULL && ++url_sp != NULL) {
        string_splitter param_sp(url_sp.field(), url_sp.field() + url_sp.length(), '&');
        while (param_sp != NULL) {
            string_splitter kv_sp(param_sp.field(), param_sp.field() + param_sp.length(), '=');
            std::string key(kv_sp.field(), kv_sp.length());
            if (kv_sp != NULL && key == "seconds" && ++kv_sp != NULL) {
                char *end_ptr;
                seconds = strtoul(kv_sp.field(), &end_ptr, 10) * 1000000;
                break;
            }
            param_sp++;
        }
    }

    resp.status_code = http_status_code::ok;

    get_cpu_profile(resp.body, seconds);

    _in_pprof_action.store(false);
}

} // namespace dsn

#endif // DSN_ENABLE_GPERF
