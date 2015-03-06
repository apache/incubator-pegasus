#pragma once

#include "meta_service.h"

class server_state;
class meta_service_app : public service_app
{
public:
    meta_service_app(service_app_spec* s, configuration_ptr c);

    ~meta_service_app();

    virtual error_code start(int argc, char** argv) override;

    virtual void stop(bool cleanup = false) override;

private:
    static server_state *_reliableState;
    meta_service*        _service;
};

