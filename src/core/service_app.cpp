# include <rdsn/internal/service_app.h>
# include <rdsn/service_api.h>

namespace rdsn { namespace service {

service_app::service_app(service_app_spec* s, configuration_ptr c)
{
    _spec = *s;
    _config = c;

    std::vector<std::string> args;
    utils::split_args(_spec.arguments.c_str(), args);

    int argc = (int)args.size() + 1;
    _argsPtr.resize(argc);
    _args.resize(argc);
    for (int i = 0; i < argc; i++)
    {
        if (0 == i)
        {
            _args[0] = _spec.type;
        }
        else
        {
            _args[i] = args[i-1];
        }

        _argsPtr[i] = ((char*)_args[i].c_str());
    }

    _address.port = (uint16_t)_spec.port;
}

service_app::~service_app(void)
{
}

void service_app::set_address(const end_point& addr)
{
    rassert (_address.port == 0 || _address.port == addr.port, "invalid service address");
    _address = addr;
}

void service_apps::add(service_app* app)
{
    _apps[app->name()] = app;
}
    
service_app* service_apps::get(const char* name) const
{
    auto it = _apps.find(name);
    if (it != _apps.end())
        return it->second;
    else
        return nullptr;
}

}} // end namespace rdsn::service_api
