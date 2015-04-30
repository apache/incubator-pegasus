
# pragma once

# include "counter.server.h"

namespace dsn {
    namespace example {

        using namespace ::dsn::replication;

        class counter_service_impl
            : public counter_service
        {
        public:
            counter_service_impl(replica* replica, configuration_ptr& config);

            virtual void on_add(const ::dsn::example::count_op& op, ::dsn::service::rpc_replier<int32_t>& reply);
            virtual void on_read(const std::string& name, ::dsn::service::rpc_replier<int32_t>& reply);

            //
            // interfaces to be implemented by app
            // all return values are error code
            //
            virtual int  open(bool create_new); // singel threaded
            virtual int  close(bool clear_state); // must be thread-safe

            // update _last_durable_decree internally
            virtual int  flush(bool force);  // must be thread-safe

            //
            // helper routines to accelerate learning
            // 
            virtual int  get_learn_state(decree start, const blob& learn_request, __out_param learn_state& state);  // must be thread-safe
            virtual int  apply_learn_state(learn_state& state);  // must be thread-safe, and last_committed_decree must equal to last_durable_decree after learning

			virtual ::dsn::replication::decree last_committed_decree() const {
				return _last_committed_decree.load();
			}
			virtual ::dsn::replication::decree last_durable_decree() const {
				return _last_durable_decree.load();
			}
        private:
            void recover();
            void recover(const std::string& name, decree version);

        private:
            ::dsn::service::zlock _lock;
            std::map<std::string, int32_t> _counters;
			
			std::atomic<decree> _last_committed_decree;
			std::atomic<decree> _last_durable_decree;
        };
    }
}

