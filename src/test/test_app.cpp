
# include "test_app.h"

namespace dsn {
    namespace service {

        test_app::test_app(service_app_spec* s, configuration_ptr c)
        : service_app(s, c), serverlet<test_app>("test_app")
        {

        }

        error_code test_app::start(int argc, char** argv)
        {
            
            return ERR_SUCCESS;
        }

        void test_app::stop(bool cleanup)
        {
            
        } 

    }
}
