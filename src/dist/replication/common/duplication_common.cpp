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

#include <dsn/dist/replication/replication_types.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/singleton.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

namespace dsn {
namespace replication {

/*extern*/ const char *duplication_status_to_string(duplication_status::type status)
{
    auto it = _duplication_status_VALUES_TO_NAMES.find(status);
    dassert(it != _duplication_status_VALUES_TO_NAMES.end(),
            "unexpected type of duplication_status: %d",
            status);
    return it->second;
}

/*extern*/ const char *get_current_cluster_name()
{
    static const char *cluster_name =
        dsn_config_get_value_string("replication", "cluster_name", "", "name of this cluster");
    dassert(strlen(cluster_name) != 0, "cluster_name is not set");
    return cluster_name;
}

namespace internal {

class duplication_group_registry : public utils::singleton<duplication_group_registry>
{
private:
    std::map<std::string, uint8_t> _group;
    std::set<uint8_t> _distinct_cids;

public:
    duplication_group_registry()
    {
        std::vector<std::string> clusters;
        dsn_config_get_all_keys("duplication-group", clusters);
        for (std::string &cluster : clusters) {
            int64_t cluster_id =
                dsn_config_get_value_int64("duplication-group", cluster.data(), 0, "");
            dassert(cluster_id < 128 && cluster_id > 0,
                    "cluster_id(%zd) for %s should be in [1, 127]",
                    cluster_id,
                    cluster.data());
            _group.emplace(cluster, static_cast<uint8_t>(cluster_id));
        }
        dassert_f(clusters.size() == _group.size(),
                  "there might be duplicate cluster_name in configuration");

        for (const auto &kv : _group) {
            _distinct_cids.insert(kv.second);
        }
        dassert_f(_distinct_cids.size() == _group.size(),
                  "there might be duplicate cluster_id in configuration");
    }

    error_with<uint8_t> get_cluster_id(const std::string &cluster_name) const
    {
        if (cluster_name.empty()) {
            return error_s::make(ERR_INVALID_PARAMETERS, "cluster_name is empty");
        }
        if (_group.empty()) {
            return error_s::make(ERR_OBJECT_NOT_FOUND, "`duplication-group` is not configured");
        }

        auto it = _group.find(cluster_name);
        if (it == _group.end()) {
            return error_s::make(ERR_OBJECT_NOT_FOUND, "failed to get cluster id for ")
                   << cluster_name.data();
        }
        return it->second;
    }

    const std::map<std::string, uint8_t> &get_duplication_group() { return _group; }
    const std::set<uint8_t> &get_distinct_cluster_id_set() { return _distinct_cids; }
};

} // namespace internal

/*extern*/ error_with<uint8_t> get_duplication_cluster_id(const std::string &cluster_name)
{
    return internal::duplication_group_registry::instance().get_cluster_id(cluster_name);
}

// TODO(wutao1): implement our C++ version of `TSimpleJSONProtocol` if there're
//               more cases for converting thrift to JSON
/*extern*/ std::string duplication_entry_to_string(const duplication_entry &dup)
{
    rapidjson::Document doc;
    doc.SetObject();
    auto &alloc = doc.GetAllocator();

    doc.AddMember("dupid", dup.dupid, alloc);
    doc.AddMember("status", rapidjson::StringRef(duplication_status_to_string(dup.status)), alloc);
    doc.AddMember("remote",
                  rapidjson::StringRef(dup.remote_address.data(), dup.remote_address.length()),
                  alloc);
    doc.AddMember("create_ts", dup.create_ts, alloc);

    doc.AddMember("progress", rapidjson::Value(), alloc);
    auto &p = doc["progress"];
    p.SetArray();
    for (const auto &kv : dup.progress) {
        rapidjson::Value part;
        part.SetObject();
        part.AddMember("pid", kv.first, alloc);
        part.AddMember("confirmed", kv.second, alloc);
        p.PushBack(std::move(part), alloc);
    }

    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    doc.Accept(writer);
    return sb.GetString();
}

/*extern*/ const std::map<std::string, uint8_t> &get_duplication_group()
{
    return internal::duplication_group_registry::instance().get_duplication_group();
}

/*extern*/ const std::set<uint8_t> &get_distinct_cluster_id_set()
{
    return internal::duplication_group_registry::instance().get_distinct_cluster_id_set();
}

} // namespace replication
} // namespace dsn
