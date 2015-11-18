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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "simple_kv.server.impl.h"
#include <fstream>
#include <sstream>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "simple.kv"

using namespace ::dsn::service;

namespace dsn {
    namespace replication {
        namespace application {
            
            simple_kv_service_impl::simple_kv_service_impl(replica* replica)
                : simple_kv_service(replica), _lock(true)
            {
                _test_file_learning = false;
                //set_delta_state_learning_supported();
            }

            // RPC_SIMPLE_KV_READ
            void simple_kv_service_impl::on_read(const std::string& key, ::dsn::rpc_replier<std::string>& reply)
            {
                zauto_lock l(_lock);
                
                auto it = _store.find(key);
                if (it == _store.end())
                {
                    reply("");
                }
                else
                {
                    dinfo("read %s, decree = %lld\n", it->second.c_str(), last_committed_decree());
                    reply(it->second);
                }
            }

            // RPC_SIMPLE_KV_WRITE
            void simple_kv_service_impl::on_write(const kv_pair& pr, ::dsn::rpc_replier<int32_t>& reply)
            {
                zauto_lock l(_lock);
                _store[pr.key] = pr.value;

                dinfo("write %s, decree = %lld\n", pr.key.c_str(), last_committed_decree() + 1);
                reply(0);
            }

            // RPC_SIMPLE_KV_APPEND
            void simple_kv_service_impl::on_append(const kv_pair& pr, ::dsn::rpc_replier<int32_t>& reply)
            {
                zauto_lock l(_lock);
                auto it = _store.find(pr.key);
                if (it != _store.end())
                    it->second.append(pr.value);
                else
                    _store[pr.key] = pr.value;

                dinfo("append %s, decree = %lld\n", pr.key.c_str(), last_committed_decree() + 1);
                reply(0);
            }
            
            int simple_kv_service_impl::open(bool create_new)
            {
                zauto_lock l(_lock);
                if (create_new)
                {
					auto& dir = data_dir();
					dsn::utils::filesystem::remove_path(dir);
					dsn::utils::filesystem::create_directory(dir);
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
					if (!dsn::utils::filesystem::remove_path(data_dir()))
					{
						dassert(false, "Fail to delete directory %s.", data_dir().c_str());
					}
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

				std::vector<std::string> sub_list;
				auto& path = data_dir();
				if (!dsn::utils::filesystem::get_subfiles(path, sub_list, false))
				{
					dassert(false, "Fail to get subfiles in %s.", path.c_str());
				}
				for (auto& fpath : sub_list)
                {
					auto&& s = dsn::utils::filesystem::get_file_name(fpath);
                    if (s.substr(0, strlen("checkpoint.")) != std::string("checkpoint."))
                        continue;

                    decree version = static_cast<decree>(atoll(s.substr(strlen("checkpoint.")).c_str()));
                    if (version > maxVersion)
                    {
                        maxVersion = version;
                        name = data_dir() + "/" + s;
                    }
                }
				sub_list.clear();

                if (maxVersion > 0)
                {
                    recover(name, maxVersion);
                }
            }

            void simple_kv_service_impl::recover(const std::string& name, decree version)
            {
                zauto_lock l(_lock);

                std::ifstream is(name.c_str(), std::ios::binary);
                if (!is.is_open())
                    return;
                
                _store.clear();

                uint64_t count;
                int magic;
                
                is.read((char*)&count, sizeof(count));
                is.read((char*)&magic, sizeof(magic)); 
                dassert(magic == 0xdeadbeef, "invalid checkpoint");

                for (uint64_t i = 0; i < count; i++)
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

                _last_durable_decree = version;
                init_last_commit_decree(version);
            }

            int simple_kv_service_impl::flush(bool force)
            {
                zauto_lock l(_lock);

                if (last_committed_decree() == last_durable_decree())
                {
                    return 0;
                }

                // TODO: should use async write instead
                char name[256];
                sprintf(name, "%s/checkpoint.%lld", data_dir().c_str(), 
                        static_cast<long long int>(last_committed_decree()));
                std::ofstream os(name, std::ios::binary);

                uint64_t count = (uint64_t)_store.size();
                int magic = 0xdeadbeef;
                
                os.write((const char*)&count, (uint32_t)sizeof(count));
                os.write((const char*)&magic, (uint32_t)sizeof(magic));

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
                return 0;
            }

            // helper routines to accelerate learning
            int simple_kv_service_impl::get_learn_state(decree start, const blob& learn_req, /*out*/ learn_state& state)
            {
                ::dsn::binary_writer writer;

                zauto_lock l(_lock);

                int magic = 0xdeadbeef;
                writer.write(magic);

                auto c = last_committed_decree();
                writer.write(c);

                dassert(c >= 0, "");

                uint64_t count = static_cast<uint64_t>(_store.size());
                writer.write(count);

                for (auto it = _store.begin(); it != _store.end(); it++)
                {
                    writer.write(it->first);
                    writer.write(it->second);
                }

                auto bb = writer.get_buffer();
                auto buf = bb.buffer();

                state.meta.push_back(blob(buf, static_cast<int>(bb.data() - bb.buffer().get()), bb.length()));
                
                // Test Sample
                if (_test_file_learning)
                {
                    std::stringstream ss;                
                    ss << dsn_random32(0, 10000);

                    auto learn_test_file = data_dir() + "/test_learning_" + ss.str() + ".txt";
                    state.files.push_back(learn_test_file);
                    
                    std::ofstream fout(learn_test_file.c_str());
                    fout << "DEADBEEF" << std::endl;        
                    fout.close();        
                }

                return 0;
            }

            int simple_kv_service_impl::apply_learn_state(learn_state& state)
            {
                blob bb((const char*)state.meta[0].data(), 0, state.meta[0].length());

                binary_reader reader(bb);

                zauto_lock l(_lock);

                _store.clear();

                int magic;
                reader.read(magic);

                dassert(magic == 0xdeadbeef, "");

                decree decree;
                reader.read(decree);

                dassert(decree >= 0, "");

                uint64_t count;
                reader.read(count);

                for (uint64_t i = 0; i < count; i++)
                {
                    std::string key, value;
                    reader.read(key);
                    reader.read(value);
                    _store[key] = value;
                }

                init_last_commit_decree(decree);
                _last_durable_decree = 0;

                flush(true);

                bool ret = true;
                if (_test_file_learning)
                {
                    dassert(state.files.size() == 1, "");
                    std::string fn = learn_dir() + "/" + state.files[0];
                    ret = dsn::utils::filesystem::path_exists(fn.c_str());
                    if (ret)
                    {
                        std::string s;
                        std::ifstream fin(fn.c_str());                        
                        
                        getline(fin, s);
                        fin.close();

                        ret = (s == "DEADBEEF");
                    }
                    else
                    {
                        dassert(false, "learning file is missing");
                    }
                }

                if (ret) return 0;
                else return ERR_LEARN_FILE_FALED.get();
            }

        }
    }
} // namespace
