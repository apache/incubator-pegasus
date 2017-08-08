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

/*
 * Description:
 *     fs_manager's implement: used to track the disk position for all the allocated replicas
 *
 * Revision history:
 *     2017-08-08: sunweijie@xiaomi.com, first draft
 */

#include "fs_manager.h"

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "fs_manager"

namespace dsn {
namespace replication {

unsigned dir_node::replicas_count() const
{
    unsigned sum = 0;
    for (const auto &s : holding_replicas) {
        sum += s.second.size();
    }
    return sum;
}

unsigned dir_node::replicas_count(app_id id) const
{
    const auto iter = holding_replicas.find(id);
    if (iter == holding_replicas.end())
        return 0;
    return iter->second.size();
}

bool dir_node::has(const gpid &pid) const
{
    auto iter = holding_replicas.find(pid.get_app_id());
    if (iter == holding_replicas.end())
        return false;
    return iter->second.find(pid) != iter->second.end();
}

unsigned dir_node::remove(const gpid &pid)
{
    auto iter = holding_replicas.find(pid.get_app_id());
    if (iter == holding_replicas.end())
        return 0;
    return iter->second.erase(pid);
}

dir_node *fs_manager::get_dir_node(const std::string &subdir)
{
    std::string norm_subdir;
    utils::filesystem::get_normalized_path(subdir, norm_subdir);
    for (auto &kv : _dir_nodes) {
        // if input is a subdir of some dir_nodes
        const std::string &d = kv.second.full_dir;
        if (norm_subdir.compare(0, d.size(), d) == 0 &&
            (norm_subdir.size() == d.size() || norm_subdir[d.size()] == '/')) {
            return &kv.second;
        }
    }
    return nullptr;
}

// size of the two vectors should be equal
dsn::error_code fs_manager::initialize(const std::vector<std::string> &data_dirs,
                                       const std::vector<std::string> &tags)
{
    // create all dir_nodes
    dassert(data_dirs.size() == tags.size(),
            "data_dir size(%u) != tags size(%u)",
            data_dirs.size(),
            tags.size());
    for (unsigned i = 0; i < data_dirs.size(); ++i) {
        auto iter = _dir_nodes.emplace(tags[i], dir_node());
        dassert(iter.second, "conflict dir tag(%s)", tags[i].c_str());
        // TODO: initialize the total storage of this dir
        iter.first->second.tag = tags[i];
        utils::filesystem::get_normalized_path(data_dirs[i], iter.first->second.full_dir);
        ddebug("%s: mark data dir(%s) as tag(%s)",
               dsn_address_to_string(dsn_primary_address()),
               data_dirs[i].c_str(),
               tags[i].c_str());
    }

    return dsn::ERR_OK;
}

dsn::error_code fs_manager::get_disk_tag(const std::string &dir, std::string &tag)
{
    dir_node *n = get_dir_node(dir);
    if (nullptr == n) {
        return dsn::ERR_OBJECT_NOT_FOUND;
    } else {
        tag = n->tag;
        return dsn::ERR_OK;
    }
}

void fs_manager::add_replica(const gpid &pid, const std::string &pid_dir)
{
    dir_node *n = get_dir_node(pid_dir);
    if (nullptr == n) {
        derror("%s: dir(%s) of gpid(%d.%d) haven't registered",
               dsn_address_to_string(dsn_primary_address()),
               pid_dir.c_str(),
               pid.get_app_id(),
               pid.get_partition_index());
    } else {
        zauto_write_lock l(_lock);
        std::set<dsn::gpid> &replicas_for_app = n->holding_replicas[pid.get_app_id()];
        auto result = replicas_for_app.emplace(pid);
        if (!result.second) {
            dwarn("%s: gpid(%d.%d) already in the dir_node(%s)",
                  dsn_address_to_string(dsn_primary_address()),
                  pid.get_app_id(),
                  pid.get_partition_index(),
                  n->tag.c_str());
        } else {
            ddebug("%s: add gpid(%d.%d) to dir_node(%s)",
                   dsn_address_to_string(dsn_primary_address()),
                   pid.get_app_id(),
                   pid.get_partition_index(),
                   n->tag.c_str());
        }
    }
}

void fs_manager::allocate_dir(const gpid &pid, const std::string &type, /*out*/ std::string &dir)
{
    char buffer[256];
    sprintf(buffer, "%d.%d.%s", pid.get_app_id(), pid.get_partition_index(), type.c_str());

    zauto_write_lock l(_lock);

    dir_node *selected = nullptr;

    unsigned least_app_replicas_count = 0;
    unsigned least_total_replicas_count = 0;

    for (auto &kv : _dir_nodes) {
        dir_node &n = kv.second;
        dassert(!n.has(pid),
                "gpid(%d.%d) already in dir_node(%s)",
                pid.get_app_id(),
                pid.get_partition_index(),
                n.tag.c_str());
        unsigned app_replicas = n.replicas_count(pid.get_app_id());
        unsigned total_replicas = n.replicas_count();

        if (selected == nullptr || least_app_replicas_count > app_replicas) {
            least_app_replicas_count = app_replicas;
            least_total_replicas_count = total_replicas;
            selected = &n;
        } else if (least_app_replicas_count == app_replicas &&
                   least_total_replicas_count > total_replicas) {
            least_total_replicas_count = total_replicas;
            selected = &n;
        }
    }

    ddebug(
        "%s: put pid(%d.%d) to dir(%s), which has %u replicas of current app, %u replicas totally",
        dsn_address_to_string(dsn_primary_address()),
        pid.get_app_id(),
        pid.get_partition_index(),
        selected->tag.c_str(),
        least_app_replicas_count,
        least_total_replicas_count);

    selected->holding_replicas[pid.get_app_id()].emplace(pid);
    dir = utils::filesystem::path_combine(selected->full_dir, buffer);
}

void fs_manager::remove_replica(const gpid &pid)
{
    zauto_write_lock l(_lock);
    unsigned remove_count = 0;
    for (auto &kv : _dir_nodes) {
        dir_node &n = kv.second;
        unsigned r = n.remove(pid);
        dassert(remove_count + r <= 1,
                "gpid(%d.%d) found in dir(%s), which was removed before",
                pid.get_app_id(),
                pid.get_partition_index(),
                n.tag.c_str());
        if (r != 0) {
            ddebug("%s: remove gpid(%d.%d) from dir(%s)",
                   dsn_address_to_string(dsn_primary_address()),
                   pid.get_app_id(),
                   pid.get_partition_index(),
                   n.tag.c_str());
        }
        remove_count += r;
    }
}

bool fs_manager::for_each_dir_node(const std::function<bool(const dir_node &)> &func) const
{
    zauto_read_lock l(_lock);
    for (const auto &kv : _dir_nodes) {
        if (!func(kv.second))
            return false;
    }
    return true;
}
}
}
