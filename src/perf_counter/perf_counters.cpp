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

#include <regex>

#include "perf_counter.h"

#include "builtin_counters.h"
#include "common/json_helper.h"
#include "perf_counter_atomic.h"
#include "perf_counter_utils.h"
#include "perf_counters.h"
#include "runtime/service_app.h"
#include "runtime/service_engine.h"
#include "runtime/task/task.h"
#include "utils/command_manager.h"
#include "utils/string_view.h"
#include "utils/time_utils.h"

namespace dsn {

perf_counters::perf_counters()
{
    // make shared_io_service destructed after perf_counters,
    // because shared_io_service will destruct the timer created by perf_counters
    // It will produce heap-use-after-free error if shared_io_service destructed in front of
    // perf_counters
    tools::shared_io_service::instance();

    _cmds.emplace_back(command_manager::instance().register_command(
        {"perf-counters"},
        "perf-counters - query perf counters, filtered by OR of POSIX basic regular expressions",
        "perf-counters [regexp]...",
        [](const std::vector<std::string> &args) {
            return perf_counters::instance().list_snapshot_by_regexp(args);
        }));
    _cmds.emplace_back(command_manager::instance().register_command(
        {"perf-counters-by-substr"},
        "perf-counters-by-substr - query perf counters, filtered by OR of substrs",
        "perf-counters-by-substr [substr]...",
        [](const std::vector<std::string> &args) {
            return perf_counters::instance().list_snapshot_by_literal(
                args, [](const std::string &arg, const counter_snapshot &cs) {
                    return cs.name.find(arg) != std::string::npos;
                });
        }));
    _cmds.emplace_back(command_manager::instance().register_command(
        {"perf-counters-by-prefix"},
        "perf-counters-by-prefix - query perf counters, filtered by OR of prefix strings",
        "perf-counters-by-prefix [prefix]...",
        [](const std::vector<std::string> &args) {
            return perf_counters::instance().list_snapshot_by_literal(
                args, [](const std::string &arg, const counter_snapshot &cs) {
                    return cs.name.size() >= arg.size() &&
                           ::memcmp(cs.name.c_str(), arg.c_str(), arg.size()) == 0;
                });
        }));
    _cmds.emplace_back(command_manager::instance().register_command(
        {"perf-counters-by-postfix"},
        "perf-counters-by-postfix - query perf counters, filtered by OR of postfix strings",
        "perf-counters-by-postfix [postfix]...",
        [](const std::vector<std::string> &args) {
            return perf_counters::instance().list_snapshot_by_literal(
                args, [](const std::string &arg, const counter_snapshot &cs) {
                    return cs.name.size() >= arg.size() &&
                           ::memcmp(cs.name.c_str() + cs.name.size() - arg.size(),
                                    arg.c_str(),
                                    arg.size()) == 0;
                });
        }));
}

perf_counters::~perf_counters()
{
    // TODO(yingchun): can we use default deconstructor?
    _counters.clear();
    _cmds.clear();
}

perf_counter_ptr perf_counters::get_app_counter(const char *section,
                                                const char *name,
                                                dsn_perf_counter_type_t flags,
                                                const char *dsptr,
                                                bool create_if_not_exist)
{
    auto cnode = task::get_current_node2();
    CHECK_NOTNULL(cnode, "cannot get current service node!");
    return get_global_counter(cnode->full_name(), section, name, flags, dsptr, create_if_not_exist);
}

perf_counter_ptr perf_counters::get_global_counter(const char *app,
                                                   const char *section,
                                                   const char *name,
                                                   dsn_perf_counter_type_t flags,
                                                   const char *dsptr,
                                                   bool create_if_not_exist)
{
    std::string full_name;
    perf_counter::build_full_name(app, section, name, full_name);

    utils::auto_write_lock l(_lock);
    if (create_if_not_exist) {
        auto it = _counters.find(full_name);
        if (it == _counters.end()) {
            perf_counter_ptr counter = new_counter(app, section, name, flags, dsptr);
            _counters.emplace(full_name, counter_object{counter, 1});
            return counter;
        } else {
            CHECK_EQ_MSG(it->second.counter->type(),
                         flags,
                         "counters with the same name {} with differnt types",
                         full_name);
            ++it->second.user_reference;
            return it->second.counter;
        }
    } else {
        auto it = _counters.find(full_name);
        if (it == _counters.end())
            return nullptr;
        else {
            ++it->second.user_reference;
            return it->second.counter;
        }
    }
}

bool perf_counters::remove_counter(const char *full_name)
{
    int remain_ref;
    {
        utils::auto_write_lock l(_lock);
        auto it = _counters.find(full_name);
        if (it == _counters.end())
            return false;
        else {
            counter_object &c = it->second;
            remain_ref = (--c.user_reference);
            if (remain_ref == 0) {
                _counters.erase(it);
            }
        }
    }

    LOG_DEBUG("performance counter %s is removed, remaining reference (%d)", full_name, remain_ref);
    return true;
}

perf_counter_ptr perf_counters::get_counter(const std::string &full_name)
{
    utils::auto_read_lock l(_lock);
    auto it = _counters.find(full_name);
    if (it != _counters.end())
        return it->second.counter;

    return nullptr;
}

perf_counter *perf_counters::new_counter(const char *app,
                                         const char *section,
                                         const char *name,
                                         dsn_perf_counter_type_t type,
                                         const char *dsptr)
{
    if (type == dsn_perf_counter_type_t::COUNTER_TYPE_NUMBER)
        return new perf_counter_number_atomic(app, section, name, type, dsptr);
    else if (type == dsn_perf_counter_type_t::COUNTER_TYPE_VOLATILE_NUMBER)
        return new perf_counter_volatile_number_atomic(app, section, name, type, dsptr);
    else if (type == dsn_perf_counter_type_t::COUNTER_TYPE_RATE)
        return new perf_counter_rate_atomic(app, section, name, type, dsptr);
    else if (type == dsn_perf_counter_type_t::COUNTER_TYPE_NUMBER_PERCENTILES)
        return new perf_counter_number_percentile_atomic(app, section, name, type, dsptr);
    else {
        CHECK(false, "invalid type({})", type);
        return nullptr;
    }
}

void perf_counters::get_all_counters(std::vector<perf_counter_ptr> *all) const
{
    all->clear();
    utils::auto_read_lock l(_lock);
    all->reserve(_counters.size());
    for (auto &p : _counters) {
        all->push_back(p.second.counter);
    }
}

std::string perf_counters::list_snapshot_by_regexp(const std::vector<std::string> &args) const
{
    perf_counter_info info;

    std::vector<std::regex> regs;
    regs.reserve(args.size());
    for (auto &arg : args) {
        try {
            regs.emplace_back(arg, std::regex_constants::basic);
        } catch (...) {
            info.result = "ERROR: invalid filter: " + arg;
            break;
        }
    }

    if (info.result.empty()) {
        snapshot_iterator visitor = [&regs, &info](const counter_snapshot &cs) {
            bool matched = false;
            if (regs.empty()) {
                matched = true;
            } else {
                for (auto &reg : regs) {
                    if (std::regex_match(cs.name, reg)) {
                        matched = true;
                        break;
                    }
                }
            }

            if (matched) {
                info.counters.emplace_back(cs.name.c_str(), cs.type, cs.value);
            }
        };
        iterate_snapshot(visitor);
        info.result = "OK";
    }

    std::stringstream ss;
    info.timestamp = _timestamp;
    char buf[20];
    utils::time_ms_to_date_time(info.timestamp * 1000, buf, sizeof(buf));
    info.timestamp_str = buf;
    info.encode_json_state(ss);
    return ss.str();
}

// the filter should return true if the counter satisfies condition.
std::string perf_counters::list_snapshot_by_literal(
    const std::vector<std::string> &args,
    std::function<bool(const std::string &arg, const counter_snapshot &cs)> filter) const
{
    perf_counter_info info;

    snapshot_iterator visitor = [&args, &info, &filter](const counter_snapshot &cs) {
        bool matched = false;
        if (args.empty()) {
            matched = true;
        } else {
            for (auto &arg : args) {
                if (filter(arg, cs)) {
                    matched = true;
                    break;
                }
            }
        }

        if (matched) {
            info.counters.emplace_back(cs.name.c_str(), cs.type, cs.value);
        }
    };
    iterate_snapshot(visitor);
    info.result = "OK";

    std::stringstream ss;
    info.timestamp = _timestamp;
    char buf[20];
    utils::time_ms_to_date_time(info.timestamp * 1000, buf, sizeof(buf));
    info.timestamp_str = buf;
    info.encode_json_state(ss);
    return ss.str();
}

void perf_counters::take_snapshot()
{
    builtin_counters::instance().update_counters();

    std::vector<perf_counter_ptr> all_counters;
    get_all_counters(&all_counters);

    utils::auto_write_lock l(_snapshot_lock);
    for (auto &p : _snapshots) {
        p.second.updated_recently = false;
    }

    // updated counters from current value
    for (const perf_counter_ptr &c : all_counters) {
        counter_snapshot &cs = _snapshots[c->full_name()];
        if (cs.name.empty()) {
            // recently created counter, which wasn't in snapshot before
            cs.name = c->full_name();
            cs.type = c->type();
        }
        cs.updated_recently = true;
        if (c->type() != COUNTER_TYPE_NUMBER_PERCENTILES) {
            cs.value = c->get_value();
        } else {
            cs.value = c->get_percentile(COUNTER_PERCENTILE_99);

            // take P999 metrics into account as well.
            std::string name_p999 = std::string(c->full_name()) + ".p999";
            counter_snapshot &cs999 = _snapshots[name_p999];
            cs999.name = std::move(name_p999);
            cs999.type = c->type();
            cs999.updated_recently = true;
            cs999.value = c->get_percentile(COUNTER_PERCENTILE_999);
        }
    }

    _timestamp = dsn_now_ms() / 1000;

    // delete old counters
    std::vector<std::string> old_counters;
    for (auto &p : _snapshots)
        if (!p.second.updated_recently)
            old_counters.push_back(p.first);
    for (const std::string &n : old_counters)
        _snapshots.erase(n);
}

void perf_counters::iterate_snapshot(const snapshot_iterator &v) const
{
    utils::auto_read_lock l(_snapshot_lock);
    for (auto &kv : _snapshots) {
        v(kv.second);
    }
}

void perf_counters::query_snapshot(const std::vector<std::string> &counters,
                                   const snapshot_iterator &v,
                                   std::vector<bool> *found) const
{
    std::vector<bool> result;
    if (found == nullptr)
        found = &result;

    found->reserve(counters.size());
    utils::auto_read_lock l(_snapshot_lock);
    for (const std::string &name : counters) {
        auto iter = _snapshots.find(name);
        if (iter == _snapshots.end()) {
            found->push_back(false);
        } else {
            found->push_back(true);
            v(iter->second);
        }
    }
}

} // namespace dsn
