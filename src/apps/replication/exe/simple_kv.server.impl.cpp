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
#include "simple_kv.server.impl.h"
#include <fstream>
#include <sstream>
#include <boost/filesystem.hpp>

namespace dsn {
    namespace replication {
        namespace application {
            
            simple_kv_service_impl::simple_kv_service_impl(replica* replica, configuration_ptr& cf)
                : simple_kv_service(replica, cf)
            {
                _learnFileName.clear();
                _last_durable_decree = 0;
                _last_committed_decree = 0;
            }

            // RPC_SIMPLE_KV_READ
            void simple_kv_service_impl::on_read(const std::string& key, ::dsn::service::rpc_replier<std::string>& reply)
            {
                zauto_lock l(_lock);

                auto it = _store.find(key);
                if (it == _store.end())
                {
                    reply("");
                }
                else
                {
                    reply(it->second);
                }
            }

            // RPC_SIMPLE_KV_WRITE
            void simple_kv_service_impl::on_write(const kv_pair& pr, ::dsn::service::rpc_replier<int32_t>& reply)
            {
                zauto_lock l(_lock);
                _store[pr.key] = pr.value;

                reply(ERR_SUCCESS);
            }

            // RPC_SIMPLE_KV_APPEND
            void simple_kv_service_impl::on_append(const kv_pair& pr, ::dsn::service::rpc_replier<int32_t>& reply)
            {
                zauto_lock l(_lock);
                auto it = _store.find(pr.key);
                if (it != _store.end())
                    it->second.append(pr.value);
                else
                    _store[pr.key] = pr.value;

                reply(ERR_SUCCESS);
            }
            
            int simple_kv_service_impl::open(bool create_new)
            {
                zauto_lock l(_lock);
                if (create_new)
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

            int simple_kv_service_impl::close(bool clear_state)
            {
                zauto_lock l(_lock);
                if (clear_state)
                {
                    boost::filesystem::remove_all(dir());
                }
                return 0;
            }

            // checkpoint related
            void simple_kv_service_impl::recover()
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

            void simple_kv_service_impl::recover(const std::string& name, decree version)
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

                _last_durable_decree = _last_committed_decree = version;
            }

            int simple_kv_service_impl::flush(bool force)
            {
                zauto_lock l(_lock);

                if (last_committed_decree() == last_durable_decree())
                {
                    return ERR_SUCCESS;
                }

                // TODO: should use async write instead
                char name[256];
                sprintf(name, "%s/checkpoint.%lld", dir().c_str(), 
                        static_cast<long long int>(last_committed_decree()));
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

                _last_durable_decree = last_committed_decree();
                return ERR_SUCCESS;
            }

            // helper routines to accelerate learning
            int simple_kv_service_impl::get_learn_state(decree start, const blob& learn_req, __out_param learn_state& state)
            {
                ::dsn::binary_writer writer;

                zauto_lock l(_lock);

                int magic = 0xdeadbeef;
                writer.write(magic);

                writer.write(_last_committed_decree.load());

                dassert(_last_committed_decree >= 0, "");

                int count = static_cast<int>(_store.size());
                writer.write(count);

                for (auto it = _store.begin(); it != _store.end(); it++)
                {
                    writer.write(it->first);
                    writer.write(it->second);
                }

                auto bb = writer.get_buffer();
                auto buf = bb.buffer();

                state.meta = blob(buf, static_cast<int>(bb.data() - bb.buffer().get()), bb.length());


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

            int simple_kv_service_impl::apply_learn_state(learn_state& state)
            {
                blob bb((const char*)state.meta.data(), 0, state.meta.length());

                binary_reader reader(bb);

                zauto_lock l(_lock);

                _store.clear();

                int magic;
                reader.read(magic);

                dassert(magic == 0xdeadbeef, "");

                decree decree;
                reader.read(decree);

                dassert(decree >= 0, "");

                int count;
                reader.read(count);

                for (int i = 0; i < count; i++)
                {
                    std::string key, value;
                    reader.read(key);
                    reader.read(value);
                    _store[key] = value;
                }

                _last_committed_decree = decree;
                _last_durable_decree = 0;

                flush(true);


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

        }
    }
} // namespace
