#include "replication_service_app.h"


namespace rdsn { namespace replication {

replication_service_app::replication_service_app(service_app_spec* s, configuration_ptr c)
    : service_app(s, c)
{
    _stub = nullptr;
}

replication_service_app::~replication_service_app(void)
{
}

error_code replication_service_app::start(int argc, char** argv)
{
    replication_options opts;
    opts.initialize(config());
    opts.WorkingDir = "./" + name();
    //initialize(opts, config, clear);

    _stub = new replica_stub();
    _stub->initialize(opts, config(), true);
    return ERR_SUCCESS;
}

void replication_service_app::stop(bool cleanup)
{
    if (_stub != nullptr)
    {
        _stub->close();
        _stub = nullptr;
    }
}

}}