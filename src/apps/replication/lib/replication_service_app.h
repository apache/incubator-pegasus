#pragma once

#include <rdsn/service_api.h>
#include "replica_stub.h"

using namespace rdsn::service;
using namespace rdsn;

namespace rdsn { namespace replication {

class replication_service_app : public rdsn::service::service_app
{
public:
    replication_service_app(service_app_spec* s, configuration_ptr c);

    ~replication_service_app(void);

    virtual error_code start(int argc, char** argv) override;

    virtual void stop(bool cleanup = false) override;

private:
    replica_stub_ptr _stub;
};

}}


