# pragma once

# include <rdsn/internal/error_code.h>
# include <rdsn/internal/end_point.h>
# include <rdsn/internal/configuration.h>
# include <rdsn/internal/global_config.h>
# include <string>

namespace rdsn { 
class service_node;    
namespace service {

class service_app
{
public:
    template <typename T> static service_app* create(service_app_spec* s, configuration_ptr c)
    {
        return new T(s, c);
    }


public:
    service_app(service_app_spec* s, configuration_ptr c);
    virtual ~service_app(void);

    virtual error_code start(int argc, char** argv) = 0;

    virtual void stop(bool cleanup = false) = 0;

    const service_app_spec& spec() const { return _spec; }
    configuration_ptr config() { return _config; }
    const std::string& name() const { return _spec.name; }
    int arg_count() const { return (int)_args.size(); }
    char** args() const { return (char**)&_argsPtr[0]; }
    const end_point& address() const { return _address; }
    service_node* svc_node() const { return _svc_node; }
    void set_address(const end_point& addr);
    void set_service_node(service_node* node) { _svc_node = node; }
        
private:    
    std::vector<std::string> _args;
    std::vector<char*>       _argsPtr;
    end_point                 _address;
    service_node*             _svc_node;

    service_app_spec           _spec;
    configuration_ptr         _config;
};

typedef service_app* (*service_app_factory)(service_app_spec*, configuration_ptr);

class service_apps : public utils::singleton<service_apps>
{
public:
    void add(service_app* app);
    
    service_app* get(const char* name) const;

    const std::map<std::string, service_app*>& get_all_apps() const { return _apps; }

private:
    std::map<std::string, service_app*> _apps;
};

}} // end namespace rdsn::service_api


