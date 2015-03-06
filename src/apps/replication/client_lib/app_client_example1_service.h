#pragma once

#include "app_client_example1.h"

using namespace rdsn;
using namespace rdsn::service;
using namespace rdsn::replication;

class app_client_example1_service : public serviceletex<app_client_example1_service>, public service_app
{
public:
    app_client_example1_service(service_app_spec* s, configuration_ptr c);

    virtual ~app_client_example1_service(void);

    virtual error_code start(int argc, char** argv) override;

    virtual void stop(bool cleanup = false) override;

private:
    void OnTimer();

private:
    app_client_example1 *_client;
    task_ptr            _writeTimer;
};

