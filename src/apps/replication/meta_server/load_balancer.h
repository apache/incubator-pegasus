#pragma once

#include "server_state.h"

using namespace rdsn;
using namespace rdsn::service;
using namespace rdsn::replication;

class load_balancer : public serviceletex<load_balancer>
{
public:
    load_balancer(server_state* state);
    ~load_balancer();

    void run();

private:
    // meta server => partition server
    void SendConfigProposal(const end_point& node, const configuration_update_request& proposal);
    void QueryDecree(boost::shared_ptr<QueryPNDecreeRequest> query);
    void OnQueryDecreeAck(error_code err, boost::shared_ptr<QueryPNDecreeRequest> query, boost::shared_ptr<QueryPNDecreeResponse> resp);
    
    void RunLB(partition_configuration& pc);
    end_point FindMinimalLoadMachine(bool primaryOnly);

private:
    server_state *_state;
};

