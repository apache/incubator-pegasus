#pragma once

# include "counter.server.h"
# include "test.common.h"

namespace dsn {
    namespace example {
        class counter_service_impl : public counter_service
        {
        public:
            virtual void on_add(const count_op& op, ::dsn::rpc_replier<count_result>& reply)
            {
                bool ok = op.operand() == TEST_ADD_ARGS_OPERAND && op.name() == TEST_ADD_ARGS_NAME;
                witness->add_result("server test on_add", ok);
                count_result result;
                result.set_value(TEST_ADD_RESULT);
                reply(result);
            }

            virtual void on_read(const count_name& args, ::dsn::rpc_replier<count_result>& reply)
            {
                bool ok = args.name() == TEST_READ_ARGS_NAME;
                witness->add_result("server test on_read", ok);
                count_result result;
                result.set_value(TEST_READ_RESULT);
                reply(result);
            }

        private:
            std::map<std::string, int32_t> _counters;
        };
    }
}
