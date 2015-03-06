#pragma once

#include "failure_detector.h"
#include "replication_common.h"

using namespace rdsn;
using namespace rdsn::service;
using namespace rdsn::replication;
using namespace rdsn::fd;

class server_state;
class meta_server_failure_detector : public failure_detector
{
public:
    meta_server_failure_detector(server_state* state);
    ~meta_server_failure_detector(void);

    virtual bool set_primary(bool isPrimary = false);
    bool is_primary() const;

    // client side
    virtual void on_master_disconnected(const std::vector<end_point>& nodes)
    {
        rdsn_assert(false, "unsupported method");
    }

    virtual void on_master_connected(const end_point& node)
    {
        rdsn_assert(false, "unsupported method");
    }

    // server side
    virtual void on_worker_disconnected(const std::vector<end_point>& nodes);
    virtual void on_worker_connected(const end_point& node);

    virtual void on_beacon(const beacon_msg& beacon, __out beacon_ack& ack);

private:
    bool        _isPrimary;
    server_state *_state;
};

