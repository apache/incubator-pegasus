#pragma once

#include <rdsn/tool_api.h>

namespace rdsn { namespace tools {

class simulator : public tool_app
{
public:
    simulator(const char* name, configuration_ptr c)
        : tool_app(name, c)
    {
    }

    void install(service_spec& s);

    virtual void run() override;
};

}} // end namespace rdsn::tools
