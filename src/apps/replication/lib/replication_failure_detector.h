#pragma once

#include "replication_common.h"
#include "failure_detector.h"


namespace rdsn { namespace replication {

class replica_stub;
class replication_failure_detector  : public rdsn::fd::failure_detector
{
public:
    replication_failure_detector(replica_stub* stub, std::vector<end_point>& meta_servers);
    ~replication_failure_detector(void);

    virtual void on_beacon_ack(error_code err, boost::shared_ptr<beacon_msg> beacon, boost::shared_ptr<beacon_ack> ack);

     // client side
    virtual void on_master_disconnected( const std::vector<end_point>& nodes );
    virtual void on_master_connected( const end_point& node);

    // server side
    virtual void on_worker_disconnected( const std::vector<end_point>& nodes ) { rdsn_assert(false, ""); }
    virtual void on_worker_connected( const end_point& node )  { rdsn_assert(false, ""); }

    end_point current_server_contact() const { zauto_lock l(_meta_lock); return _current_meta_server; }
    std::vector<end_point> get_servers() const  { zauto_lock l(_meta_lock); return _meta_servers; }

private:
    end_point find_next_meta_server(end_point current);

private:
    typedef std::set<end_point, end_point_comparor> end_points;

    mutable zlock             _meta_lock;
    end_point               _current_meta_server;

    std::vector<end_point>  _meta_servers;
    replica_stub               *_stub;
};

}} // end namespace

