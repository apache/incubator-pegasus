#pragma once

# include "counter.server.h"
# include "test.common.h"

namespace dsn {
    namespace example {
        class counter_service_impl : public counter_service
        {
        public:
            virtual void on_add(const counter_add_args& args, ::dsn::rpc_replier<counter_add_result>& reply)
            {
                bool ok = args.op.operand == TEST_ADD_ARGS_OPERAND && args.op.name == TEST_ADD_ARGS_NAME;
                witness->add_result("server test on_add", ok);
                counter_add_result result;
                result.success = TEST_ADD_RESULT;
                reply(result);
            }

            virtual void on_read(const counter_read_args& args, ::dsn::rpc_replier<counter_read_result>& reply)
            {
                bool ok = args.name == TEST_READ_ARGS_NAME;
                witness->add_result("server test on_read", ok);
                counter_read_result result;
                result.success =TEST_READ_RESULT;
                reply(result);
            }

        private:
            std::map<std::string, int32_t> _counters;
        };
    }
}
