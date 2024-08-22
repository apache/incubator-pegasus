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

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "http/http_method.h"
#include "http/http_server.h"
#include "http/http_status_code.h"
#include "runtime/api_layer1.h"
#include "utils/api_utilities.h"
#include "utils/blob.h"
#include "utils/defer.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/string_conv.h"
#include "utils/string_splitter.h"
#include "utils/strings.h"
#include "utils/timer.h"

namespace dsn {

bool check_TCMALLOC_SAMPLE_PARAMETER()
{
    char *str = getenv("TCMALLOC_SAMPLE_PARAMETER");
    if (str == nullptr) {
        return false;
    }
    char *endptr;
    int val = strtol(str, &endptr, 10);
    return (*endptr == '\0' && val > 0);
}

bool has_TCMALLOC_SAMPLE_PARAMETER()
{
    static bool val = check_TCMALLOC_SAMPLE_PARAMETER();
    return val;
}

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
    SCOPED_LOG_TIMING(INFO, "load {}", lib_info.path);
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
    return 0;
}

static void load_symbols()
{
    SCOPED_LOG_TIMING(INFO, "load all symbols");
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

    size_t num_removed = 0;
    LOG_TIMING_IF(INFO, num_removed > 0, "removed {} entries", num_removed);
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

    if (req.method != http_method::POST) {
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
        resp.status_code = http_status_code::kInternalServerError;
        return;
    }
    auto cleanup = dsn::defer([this]() { _in_pprof_action.store(false); });

    // If "seconds" parameter is specified with a valid value, use heap profiling,
    // otherwise, use heap sampling.
    bool use_heap_profile = false;
    uint32_t seconds = 0;
    const auto &iter = req.query_args.find("seconds");
    if (iter != req.query_args.end() && buf2uint32(iter->second, seconds)) {
        // This is true between calls to HeapProfilerStart() and HeapProfilerStop(), and
        // also if the program has been run with HEAPPROFILER, or some other
        // way to turn on whole-program profiling.
        if (IsHeapProfilerRunning()) {
            LOG_WARNING("heap profiling is running, dump the full profile directly");
            char *profile = GetHeapProfile();
            resp.status_code = http_status_code::kOk;
            resp.body = profile;
            free(profile);
            return;
        }

        std::stringstream profile_name_prefix;
        profile_name_prefix << "heap_profile." << getpid() << "." << dsn_now_ns();

        HeapProfilerStart(profile_name_prefix.str().c_str());
        sleep(seconds);
        char *profile = GetHeapProfile();
        HeapProfilerStop();

        resp.status_code = http_status_code::kOk;
        resp.body = profile;
        free(profile);
    } else {
        if (!has_TCMALLOC_SAMPLE_PARAMETER()) {
            static const std::string kNoEnvMsg = "The environment variable "
                                                 "TCMALLOC_SAMPLE_PARAMETER should set to a "
                                                 "positive value, such as 524288, before running.";
            LOG_WARNING(kNoEnvMsg);
            resp.status_code = http_status_code::kInternalServerError;
            resp.body = kNoEnvMsg;
            return;
        }

        std::string buf;
        MallocExtension::instance()->GetHeapSample(&buf);
        resp.status_code = http_status_code::kOk;
        resp.body = std::move(buf);
    }
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
        resp.status_code = http_status_code::kInternalServerError;
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
static bool get_cpu_profile(std::string &result, useconds_t micro_seconds)
{
    const char *file_name = "cpu.prof";

    ProfilerStart(file_name);
    usleep(micro_seconds);
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
        resp.status_code = http_status_code::kInternalServerError;
        return;
    }

    useconds_t micro_seconds = 60000000;

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
                micro_seconds = strtoul(kv_sp.field(), &end_ptr, 10) * 1000000;
                break;
            }
            param_sp++;
        }
    }

    resp.status_code = http_status_code::kOk;

    get_cpu_profile(resp.body, micro_seconds);

    _in_pprof_action.store(false);
}

} // namespace dsn

#endif // DSN_ENABLE_GPERF
