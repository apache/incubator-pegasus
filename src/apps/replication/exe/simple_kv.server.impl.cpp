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
                    dinfo("read %s, decree = %" PRId64 "\n", it->second.c_str(), last_committed_decree());
                    reply(it->second);
                }
            }

            // RPC_SIMPLE_KV_WRITE
            void simple_kv_service_impl::on_write(const kv_pair& pr, ::dsn::rpc_replier<int32_t>& reply)
            {
                zauto_lock l(_lock);
                _store[pr.key] = pr.value;

                dinfo("write %s, decree = %" PRId64 "\n", pr.key.c_str(), last_committed_decree() + 1);
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

                dinfo("append %s, decree = %" PRId64 "\n", pr.key.c_str(), last_committed_decree() + 1);
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

            int simple_kv_service_impl::checkpoint()
            {
                zauto_lock l(_lock);

                if (last_committed_decree() == last_durable_decree())
                {
                    return 0;
                }

                // TODO: should use async write instead
                char name[256];
                sprintf(name, "%s/checkpoint.%" PRId64, data_dir().c_str(),
                        last_committed_decree());
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

                // TODO: gc checkpoints

                _last_durable_decree = last_committed_decree();
                return 0;
            }

            // helper routines to accelerate learning
            int simple_kv_service_impl::get_checkpoint(decree start, const blob& learn_req, /*out*/ learn_state& state)
            {
                if (_last_durable_decree.load() > 0)
                {
                    state.from_decree_excluded = 0;
                    state.to_decree_included = _last_durable_decree;

                    char name[256];
                    sprintf(name, "%s/checkpoint.%" PRId64,
                        data_dir().c_str(),
                        _last_durable_decree.load()
                        );

                    state.files.push_back(name);
                    return ERR_OK;
                }
                else
                {
                    state.from_decree_excluded = 0;
                    state.to_decree_included = 0;
                    return ERR_OBJECT_NOT_FOUND;
                }
            }

            int simple_kv_service_impl::apply_checkpoint(learn_state& state, chkpt_apply_mode mode)
            {
                if (mode == CHKPT_LEARN)
                {
                    recover(state.files[0], state.to_decree_included);
                    return ERR_OK;
                }
                else
                {
                    dassert(CHKPT_COPY == mode, "invalid mode %d", (int)mode);
                    dassert(state.to_decree_included > _last_durable_decree, "checkpoint's decree is smaller than current");

                    char name[256];
                    sprintf(name, "%s/checkpoint.%" PRId64,
                        data_dir().c_str(),
                        state.to_decree_included
                        );
                    std::string lname(name);

                    if (!utils::filesystem::rename_path(state.files[0], lname))
                        return ERR_CHECKPOINT_FAILED;
                    else
                    {
                        _last_durable_decree = state.to_decree_included;
                        return ERR_OK;
                    }                        
                }
            }

        }
    }
} // namespace
