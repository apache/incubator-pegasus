# include "replica.h"
# include "replica_stub.h"
# include <dsn/tool/simulator.h>

namespace dsn {
    namespace replication {

        class invariance_rules_checker : ::dsn::tools::checker
        {


            virtual bool check()
            {

            }
        };

        void on_sys_init_after_apps_created_to_inject_replication_checkers(configuration_ptr config)
        {
            auto sim = dynamic_cast<::dsn::tools::simulator*>(::dsn::tools::get_current_tool());
            if (nullptr == sim)
                return;


        }
    }
}
