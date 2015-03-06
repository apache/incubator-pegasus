#include "meta_service_app.h"
#include "server_state.h"

server_state * meta_service_app::_reliableState = nullptr;

meta_service_app::meta_service_app(service_app_spec* s, configuration_ptr c)
    : service_app(s, c)
{
    _service = nullptr;
}

meta_service_app::~meta_service_app()
{

}

error_code meta_service_app::start(int argc, char** argv)
{
    if (nullptr == _reliableState)
    {
        _reliableState = new server_state();
    }

    _service = new meta_service(_reliableState, config());
    _reliableState->AddMetaNode(_service->address());
    _service->start();    
    return ERR_SUCCESS;
}

void meta_service_app::stop(bool cleanup)
{
    if (_reliableState != nullptr)
    {
        if (_service != nullptr)
        {
            _service->stop();
            _reliableState->RemoveMetaNode(_service->address());
            delete _service;
            _service = nullptr;

            end_point primary;
            if (!_reliableState->GetMetaServerPrimary(primary))
            {
                delete _reliableState;
                _reliableState = nullptr;
            }
        }
    }
    else
    {
        rassert(_service == nullptr, "service must be null");
    }
}
