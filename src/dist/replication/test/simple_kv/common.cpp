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
 *     Replication testing framework.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "common.h"
# include "checker.h"

# include <dsn/cpp/utils.h>

# include <sstream>
# include <boost/lexical_cast.hpp>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "simple_kv.common"

namespace dsn { namespace replication { namespace test {

std::string g_case_input("case-000.act");
gpid g_default_gpid(1, 0);
bool g_done = false;
bool g_fail = false;

const char* partition_status_to_short_string(partition_status::type s)
{
    switch(s)
    {
    case partition_status::PS_INACTIVE:
        return "ina";
    case partition_status::PS_ERROR:
        return "err";
    case partition_status::PS_PRIMARY:
        return "pri";
    case partition_status::PS_SECONDARY:
        return "sec";
    case partition_status::PS_POTENTIAL_SECONDARY:
        return "pot";
    case partition_status::PS_INVALID:
        return "inv";
    default:
        dassert(false, "");
        return "";
    }
}

partition_status::type partition_status_from_short_string(const std::string& str)
{
    if (str == "ina") return partition_status::PS_INACTIVE;
    if (str == "err") return partition_status::PS_ERROR;
    if (str == "pri") return partition_status::PS_PRIMARY;
    if (str == "sec") return partition_status::PS_SECONDARY;
    if (str == "pot") return partition_status::PS_POTENTIAL_SECONDARY;
    if (str == "inv") return partition_status::PS_INVALID;
    dassert(false, "");
    return partition_status::PS_INVALID;
}

std::string address_to_node(rpc_address addr)
{
    if (addr.is_invalid()) return "-";
    dassert(test_checker::s_inited, "");
    return test_checker::fast_instance().address_to_node_name(addr);
}

rpc_address node_to_address(const std::string& name)
{
    if (name == "-") return rpc_address();
    dassert(test_checker::s_inited, "");
    return test_checker::fast_instance().node_name_to_address(name);
}

std::string gpid_to_string(gpid gpid)
{
    std::stringstream oss;
    oss << gpid.get_app_id() << "." << gpid.get_partition_index();
    return oss.str();
}

bool gpid_from_string(const std::string& str, gpid& gpid)
{
    size_t pos = str.find('.');
    if (pos == std::string::npos)
        return false;
    gpid.set_app_id(boost::lexical_cast<int32_t>(str.substr(0, pos)));
    gpid.set_partition_index(boost::lexical_cast<int32_t>(str.substr(pos + 1)));
    return true;
}

std::string replica_id::to_string() const
{
    std::stringstream oss;
#ifdef ENABLE_GPID
    oss << gpid_to_string(gpid) << "@" << node;
#else
    oss << node;
#endif
    return oss.str();
}

bool replica_id::from_string(const std::string& str)
{
    if (str.empty())
        return false;
#ifdef ENABLE_GPID
    size_t pos = str.find('@');
    if (pos == std::string::npos)
        return false;
    if (!gpid_from_string(str.substr(0, pos), gpid))
        return false;
    node = str.substr(pos + 1);
    if (node.empty())
        return false;
#else
    node = str;
#endif
    return true;
}

std::string replica_state::to_string() const
{
    std::stringstream oss;
    oss << "{"
        << id.to_string() << ","
        << partition_status_to_short_string(status) << ","
        << ballot << ","
        << last_committed_decree;
    if (last_durable_decree != -1)
        oss << "," << last_durable_decree;
    oss << "}";
    return oss.str();
}

//{r3,sec,3,0} or {r3,sec,3,1,0}
bool replica_state::from_string(const std::string& str)
{
    if (str.size() < 2 || str[0] != '{' || str[str.size()-1] != '}')
        return false;
    std::string s = str.substr(1, str.size()-2);
    std::vector<std::string> splits;
    dsn::utils::split_args(s.c_str(), splits, ',');
    if (splits.size() != 4 && splits.size() != 5)
        return false;
    if (!id.from_string(splits[0]))
        return false;
    status = partition_status_from_short_string(splits[1]);
    ballot = boost::lexical_cast<int64_t>(splits[2]);
    last_committed_decree = boost::lexical_cast<decree>(splits[3]);
    if (splits.size() == 5)
        last_durable_decree = boost::lexical_cast<decree>(splits[4]);
    return true;
}

std::string state_snapshot::to_string() const
{
    std::stringstream oss;
    oss << "{";
    int i = 0;
    for (auto& kv : state_map)
    {
        const replica_state& s = kv.second;
        if (i != 0)
            oss << ",";
        oss << s.to_string();
        i++;
    }
    oss << "}";
    return oss.str();
}

//{{r1,pri,3,0},{r2,sec,3,0},{r3,sec,3,0}}
bool state_snapshot::from_string(const std::string& str)
{
    if (str.size() < 2 || str[0] != '{' || str[str.size()-1] != '}')
        return false;
    state_map.clear();
    std::string s = str.substr(1, str.size()-2);
    std::vector<std::string> splits;
    dsn::utils::split_args(s.c_str(), splits, '{');
    for (std::string& i : splits)
    {
        if (i.empty())
            continue;
        if (i[i.size()-1] == ',')
            i.resize(i.size()-1);
        std::string x = "{"+i;
        replica_state v;
        if (!v.from_string(x))
            return false;
        if (state_map.find(v.id) != state_map.end())
            return false;
        state_map[v.id] = v;
    }
    return true;
}

std::string state_snapshot::diff_string(const state_snapshot& other) const
{
    auto& oth = other.state_map;
    auto& cur = this->state_map;

    const char* add_mark = "  + ";
    const char* del_mark = "  - ";
    const char* chg_mark = "  x ";
    const char* unc_mark = "    ";

    auto oth_it = oth.begin();
    auto cur_it = cur.begin();
    std::stringstream oss;
    oss << "{" << std::endl;
    while (oth_it != oth.end() && cur_it != cur.end())
    {
        if (oth_it->first < cur_it->first)
        {
            oss << del_mark << oth_it->second.to_string() << std::endl;
            ++oth_it;
        }
        else if (cur_it->first < oth_it->first)
        {
            oss << add_mark << cur_it->second.to_string() << std::endl;
            ++cur_it;
        }
        else
        {
            dassert(oth_it->first == cur_it->first, "");
            if (oth_it->second != cur_it->second)
            {
                oss << chg_mark << cur_it->second.to_string()
                          << " <= " << oth_it->second.to_string() << std::endl;
            }
            else
            {
                oss << unc_mark << cur_it->second.to_string() << std::endl;
            }
            ++oth_it;
            ++cur_it;
        }
    }
    while (oth_it != oth.end())
    {
        oss << del_mark << oth_it->second.to_string() << std::endl;
        ++oth_it;
    }
    while (cur_it != cur.end())
    {
        oss << add_mark << cur_it->second.to_string() << std::endl;
        ++cur_it;
    }
    oss << "}";

    return oss.str();
}

std::string parti_config::to_string() const
{
    std::stringstream oss;
    oss << "{"
#ifdef ENABLE_GPID
        << gpid_to_string(gpid) << ","
#endif
        << ballot << ","
        << primary << ",[";
    for (size_t i = 0; i < secondaries.size(); ++i)
    {
        if (i != 0)
            oss << ",";
        oss << secondaries[i];
    }
    oss << "]}";
    return oss.str();
}

//{3,r1,[r2,r3],0}
bool parti_config::from_string(const std::string& str)
{
    if (str.size() < 2 || str[0] != '{' || str[str.size()-1] != '}')
        return false;
    std::string s = str.substr(1, str.size()-2);
    // replace ',' in [] to ';'
    size_t pos1 = s.find('[');
    size_t pos2 = s.find(']');
    if (pos1 == std::string::npos || pos2 == std::string::npos || pos1 > pos2)
        return false;
    for (size_t i = pos1 + 1; i < pos2; ++i)
    {
        if (s[i] == ',')
            s[i] = ';';
    }
    std::vector<std::string> splits;
    dsn::utils::split_args(s.c_str(), splits, ',');
    size_t i = 0;
#ifdef ENABLE_GPID
    // gpid
    if (!gpid_from_string(splits[i++], gpid))
        return false;
#endif
    // ballot
    ballot = boost::lexical_cast<int64_t>(splits[i++]);
    // primary
    primary = splits[i++];
    // secondaries
    std::string sec = splits[i++];
    if (sec.size() < 2 || sec[0] != '[' || sec[sec.size()-1] != ']')
        return false;
    dsn::utils::split_args(sec.substr(1, sec.size()-2).c_str(), secondaries, ';');
    std::sort(secondaries.begin(), secondaries.end());
    if (i != splits.size())
        return false;
    return true;
}

void parti_config::convert_from(const partition_configuration& c)
{
    pid = c.pid;
    ballot = c.ballot;
    primary = address_to_node(c.primary);
    for (auto& s : c.secondaries)
        secondaries.push_back(address_to_node(s));
    std::sort(secondaries.begin(), secondaries.end());
}

}}}

