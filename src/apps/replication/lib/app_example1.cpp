/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include "app_example1.h"
#include <fstream>
#include <sstream>
#include <boost/filesystem.hpp>

namespace rdsn { namespace replication {

replication_app_base* create_simplekv_app(replica* replica, configuration_ptr c)
{
    static replication_app_example1_config * appConfig = nullptr;
    if (appConfig == nullptr)
    {
        appConfig = new replication_app_example1_config();
        if (!appConfig->initialize(c))
            return nullptr;
    }

    return new replication_app_example1(replica, appConfig);
}

replication_app_example1::replication_app_example1(replica* replica, const replication_app_config* c)
    : replication_app_base(replica, c)
{
    _lastCommittedDecree = 0;
    _lastDurableDecree = 0;
    _learnFileName.clear();
}

int replication_app_example1::write(std::list<message_ptr>& requests, decree decree, bool ackClient)
{
    rassert(_lastCommittedDecree + 1 == decree, "");
    _lastCommittedDecree = decree;

    for (auto it = requests.begin(); it != requests.end(); it++)
    {
        message_ptr request = *it;
        SimpleKvRequest msg;
        
        unmarshall(request, msg);

        int error;
        {
        zauto_lock l(_lock);
        switch (msg.op)
        {
        case SimpleKvOperation::SKV_UPDATE:
            _store[msg.key] = msg.value;
            error = ERR_SUCCESS;
            break;
        case SimpleKvOperation::SKV_APPEND:
            {
            auto it = _store.find(msg.key);
            if (it == _store.end())
            {
                _store[msg.key] = msg.value;
                error = ERR_SUCCESS;
            }
            else
            {
                error = ERR_SUCCESS;
                it->second.append(msg.value);
            }
            }
            break;
        default:
            rassert(false, "");
        }    
        }

        if (ackClient)
        {
            SimpleKvResponse resp;
            resp.err = error;
            resp.key = msg.key;
        
            rpc_response(request, resp);
        }
    }
    return ERR_SUCCESS;
}

void replication_app_example1::read(const client_read_request& meta, rdsn::message_ptr& request)
{
    SimpleKvRequest msg;
    unmarshall(request, msg);

    rassert(msg.op == SimpleKvOperation::SKV_READ, "");

    SimpleKvResponse resp;
    resp.key = msg.key;

    {
    zauto_lock l(_lock);

    if (meta.semantic == ReadSnapshot &&
        meta.versionDecree != last_committed_decree())
    {
        resp.err = ERR_INVALID_VERSION;
    }
    else
    {
        auto it = _store.find(msg.key);
        if (it == _store.end())
        {
            resp.err = ERR_OBJECT_NOT_FOUND;
        }
        else
        {
            resp.err = ERR_SUCCESS;
            resp.value = it->second;
        }
    }
    }

    rpc_response(request, resp);
}

int replication_app_example1::open(bool createNew)
{
    zauto_lock l(_lock);
    if (createNew)
    {
        boost::filesystem::remove_all(dir());
        boost::filesystem::create_directory(dir());
    }
    else
    {
        recover();
    }
    return 0;
}

int replication_app_example1::close(bool clearState)
{
    zauto_lock l(_lock);
    if (clearState)
    {
        boost::filesystem::remove_all(dir());
    }
    return 0;
}

// checkpoint related
void replication_app_example1::recover()
{
    zauto_lock l(_lock);

    _store.clear();

    decree maxVersion = 0;
    std::string name;
    boost::filesystem::directory_iterator endtr;
    for (boost::filesystem::directory_iterator it(dir());
        it != endtr;
        ++it)
    {
        auto s = it->path().filename().string();
        if (s.substr(0, strlen("checkpoint.")) != std::string("checkpoint."))
            continue;

        decree version = atol(s.substr(strlen("checkpoint.")).c_str());
        if (version > maxVersion)
        {
            maxVersion = version;
            name = dir() + "/" + s;
        }
    }

    if (maxVersion > 0)
    {
        recover(name, maxVersion);
    }
}

void replication_app_example1::recover(const std::string& name, decree version)
{
    zauto_lock l(_lock);

    std::ifstream is(name.c_str());
    if (!is.is_open())
        return;

    
    _store.clear();

    uint32_t count;
    is.read((char*)&count, sizeof(count));

    for (uint32_t i = 0; i < count; i++)
    {
        std::string key;
        std::string value;

        uint32_t sz;
        is.read((char*)&sz, (uint32_t)sizeof(sz));
        key.resize(sz);

        is.read((char*)&key[0], sz);

        is.read((char*)&sz, (uint32_t)sizeof(sz));
        value.resize(sz);

        is.read((char*)&value[0], sz);

        _store[key] = value;
    }

    _lastCommittedDecree = _lastDurableDecree = version;
}
    
int replication_app_example1::compact(bool force)
{
    zauto_lock l(_lock);

    if (last_committed_decree() == last_durable_decree())
    {
        return ERR_SUCCESS;
    }

    // TODO: should use async write instead
    char name[256];
    sprintf(name, "%s/checkpoint.%llu", dir().c_str(), last_committed_decree());
    std::ofstream os(name);

    uint32_t count = (uint32_t)_store.size();
    os.write((const char*)&count, (uint32_t)sizeof(count));

    for (auto it = _store.begin(); it != _store.end(); it++)
    {
        const std::string& k = it->first;
        uint32_t sz = (uint32_t)k.length();

        os.write((const char*)&sz, (uint32_t)sizeof(sz));
        os.write((const char*)&k[0], sz);

        const std::string& v = it->second;
        sz = (uint32_t)v.length();

        os.write((const char*)&sz, (uint32_t)sizeof(sz));
        os.write((const char*)&v[0], sz);
    }

    _lastDurableDecree = last_committed_decree();
    return ERR_SUCCESS;
}

// helper routines to accelerate learning
int replication_app_example1::get_learn_state(decree start, const utils::blob& learnRequest, __out learn_state& state)
{
    rdsn::utils::binary_writer writer;

    zauto_lock l(_lock);

    int magic = 0xdeadbeef;
    writer.write(magic);

    writer.write(_lastCommittedDecree);

    rassert(_lastCommittedDecree >= 0, "");
    
    int count = (int)_store.size();
    writer.write(count);

    for (auto it = _store.begin(); it != _store.end(); it++)
    {
        writer.write(it->first);
        writer.write(it->second);
    }
    
    auto bb = writer.get_buffer();
    auto buf = bb.buffer();

    state.meta = utils::blob(buf, (int)(bb.data() - bb.buffer().get()), bb.length());
       

    //// Test Sample
    //if (_learnFileName.empty())
    //{
    //    std::stringstream ss;                
    //    ss << std::rand();
    //    ss >> _learnFileName;        
    //    _learnFileName = dir() + "/test_transfer_" + _learnFileName + ".txt";        
    //    
    //    std::ofstream fout(_learnFileName.c_str());
    //    fout << "Test by Kit" << std::endl;        
    //    fout.close();        
    //}
    //
    //state.files.push_back(_learnFileName);
    
    return ERR_SUCCESS;
}

int replication_app_example1::apply_learn_state(learn_state& state)
{
    utils::blob bb((const char*)state.meta.data(), 0, state.meta.length());

    utils::binary_reader reader(bb);

    zauto_lock l(_lock);

    _store.clear();

    int magic;
    reader.read(magic);

    rassert(magic == 0xdeadbeef, "");

    decree decree;
    reader.read(decree);

    rassert(decree >= 0, "");

    int count;
    reader.read(count);

    for (int i = 0; i < count; i++)
    {
        std::string key, value;
        reader.read(key);
        reader.read(value);
        _store[key] = value;
    }

    _lastCommittedDecree = decree;
    _lastDurableDecree = 0;

    compact(true);

    
    bool ret = true;
    for (auto itr = state.files.begin(); itr != state.files.end(); ++itr)
        if (itr->find("test_transfer") != std::string::npos)
        {
            std::string fn = dir() + "/" + *itr;
            ret = boost::filesystem::exists(fn);
            if (!ret) break;

            std::ifstream fin(fn.c_str());
            std::string s;
            getline(fin, s);
            fin.close();
            ret = (s == "Test by Kit");
            // FileUtil::RemoveFile(fn.c_str());
        }        

    if (ret) return ERR_SUCCESS;
    else return ERR_LEARN_FILE_FALED;
}

}} // namespace
