#pragma once

#include <rdsn/tool_api.h>

namespace rdsn {
    namespace tools {

        class tracer : public toollet
        {
        public:
            tracer(const char* name, configuration_ptr config);
            virtual void install(service_spec& spec);
        };
    }
}


