#pragma once

#include <rdsn/tool_api.h>

namespace rdsn {
    namespace tools {

        class fault_injector : public toollet
        {
        public:
            fault_injector(const char* name, configuration_ptr config);
            virtual void install(service_spec& spec);
        };
    }
}


