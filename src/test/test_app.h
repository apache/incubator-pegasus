
#pragma once

# include <dsn/serverlet.h>

namespace dsn {
    namespace service {

        class test_app : public serverlet<test_app>, public service_app
        {
        public:
            test_app(service_app_spec* s, configuration_ptr c);
            virtual error_code start(int argc, char** argv);
            virtual void stop(bool cleanup = false);
        };
    }
}
