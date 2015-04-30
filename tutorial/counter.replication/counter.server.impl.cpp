
# include "counter.server.impl.h"
# include <boost/filesystem.hpp>

namespace dsn {
    namespace example {


        counter_service_impl::counter_service_impl(replica* replica, configuration_ptr& config)
            : counter_service(replica, config)
        {
			_last_durable_decree = 0;
			_last_committed_decree = 0;
        }

        void counter_service_impl::on_add(const ::dsn::example::count_op& op, ::dsn::service::rpc_replier<int32_t>& reply)
        {
            zauto_lock l(_lock);
            auto rt = _counters[op.name] += op.operand;
            reply(rt);
        }

        void counter_service_impl::on_read(const std::string& name, ::dsn::service::rpc_replier<int32_t>& reply)
        {
            zauto_lock l(_lock);

            auto it = _counters.find(name);
            if (it == _counters.end())
            {
                reply(0);
            }
            else
            {
                reply(it->second);
            }
        }
                
        int counter_service_impl::open(bool create_new)
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

        int counter_service_impl::close(bool clear_state)
        {
            zauto_lock l(_lock);
            if (clear_state)
            {
                boost::filesystem::remove_all(dir());
            }
            return 0;
        }

        // checkpoint related
        void counter_service_impl::recover()
        {
            zauto_lock l(_lock);

            _counters.clear();

            decree max_ver = 0;
            std::string name;
            boost::filesystem::directory_iterator end_it;
            for (boost::filesystem::directory_iterator it(dir());
                it != end_it;
                ++it)
            {
                auto s = it->path().filename().string();
                if (s.substr(0, strlen("checkpoint.")) != std::string("checkpoint."))
                    continue;

                decree version = atol(s.substr(strlen("checkpoint.")).c_str());
                if (version > max_ver)
                {
                    max_ver = version;
                    name = dir() + "/" + s;
                }
            }

            if (max_ver > 0)
            {
                recover(name, max_ver);
            }
        }

        void counter_service_impl::recover(const std::string& name, decree version)
        {
            zauto_lock l(_lock);

            std::ifstream is(name.c_str());
            if (!is.is_open())
                return;


            _counters.clear();

            uint32_t count;
            is.read((char*)&count, sizeof(count));

            for (uint32_t i = 0; i < count; i++)
            {
                std::string key;
                int32_t     value;

                uint32_t sz;
                is.read((char*)&sz, (uint32_t)sizeof(sz));
                key.resize(sz);
                is.read((char*)&key[0], sz);

                is.read((char*)&value, sizeof(value));

                _counters[key] = value;
            }

            _last_durable_decree = _last_committed_decree = version;
        }

        int counter_service_impl::flush(bool force)
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

            uint32_t count = (uint32_t)_counters.size();
            os.write((const char*)&count, (uint32_t)sizeof(count));

            for (auto it = _counters.begin(); it != _counters.end(); it++)
            {
                const std::string& k = it->first;
                uint32_t sz = (uint32_t)k.length();

                os.write((const char*)&sz, (uint32_t)sizeof(sz));
                os.write((const char*)&k[0], sz);
                os.write((const char*)&it->second, sizeof(int32_t));
            }

            _last_durable_decree = last_committed_decree();
            return ERR_SUCCESS;
        }

        // helper routines to accelerate learning
        int counter_service_impl::get_learn_state(decree start, const blob& learn_request, __out_param learn_state& state)
        {
            ::dsn::binary_writer writer;

            zauto_lock l(_lock);

            int magic = 0xdeadbeef;
            writer.write(magic);

            writer.write(_last_committed_decree.load());

            dassert(_last_committed_decree >= 0, "");

            int count = static_cast<int>(_counters.size());
            writer.write(count);

            for (auto it = _counters.begin(); it != _counters.end(); it++)
            {
                writer.write(it->first);
                writer.write(it->second);
            }

            auto bb = writer.get_buffer();
            auto buf = bb.buffer();

            state.meta = blob(buf, static_cast<int>(bb.data() - bb.buffer().get()), bb.length());

            return ERR_SUCCESS;
        }

        int counter_service_impl::apply_learn_state(learn_state& state)
        {
            blob bb((const char*)state.meta.data(), 0, state.meta.length());

            binary_reader reader(bb);

            zauto_lock l(_lock);

            _counters.clear();

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
                std::string key;
                int32_t value;
                reader.read(key);
                reader.read(value);
                _counters[key] = value;
            }

            _last_committed_decree = decree;
            _last_durable_decree = 0;

            return flush(true);
        }
    }
}