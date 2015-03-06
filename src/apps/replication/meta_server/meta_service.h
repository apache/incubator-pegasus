#pragma once

#include "replication_common.h"

using namespace rdsn;
using namespace rdsn::service;
using namespace rdsn::replication;

class server_state;
class load_balancer;
class meta_server_failure_detector;
class meta_service : public serviceletex<meta_service>
{
public:
    meta_service(server_state* state, configuration_ptr c);
    ~meta_service(void);

    void start();
    bool stop();

private:
    void OnMetaServiceRequest(message_ptr& request);

    // partition server & client => meta server
    void OnQueryConfig(ConfigurationNodeQueryRequest& request, __out ConfigurationNodeQueryResponse& response);
    void DoQueryConfigurationByIndexRequest(QueryConfigurationByIndexRequest& request, __out QueryConfigurationByIndexResponse& response);
    void update_configuration(configuration_update_request& request, __out ConfigurationUpdateResponse& response);
   
   
    // local timers
    void OnLoadBalancerTimer();

private:
    meta_server_failure_detector *_livenessMonitor;
    server_state               *_state;
    load_balancer              *_balancer;
    task_ptr                    _balancerTimer;
    replication_options         _opts;
};

