#include "replication_failure_detector.h"
#include "replica_stub.h"

namespace rdsn { namespace replication {


replication_failure_detector::replication_failure_detector(replica_stub* stub, std::vector<end_point>& meta_servers)
    : failure_detector((stub->name() + std::string(".failure_detector")).c_str())
{
    _stub = stub;
    _meta_servers = meta_servers;
    _current_meta_server = _meta_servers[random32(0, 100) % _meta_servers.size()];
}

replication_failure_detector::~replication_failure_detector(void)
{

}

end_point replication_failure_detector::find_next_meta_server(end_point current)
{
    if (end_point::INVALID == current)
        return _meta_servers[random32(0, 100) % _meta_servers.size()];
    else
    {
        auto it = std::find(_meta_servers.begin(), _meta_servers.end(), current);
        rassert(it != _meta_servers.end(), "");
        it++;
        if (it != _meta_servers.end())
            return *it;
        else
            return _meta_servers.at(0);
    }
}

void replication_failure_detector::on_beacon_ack(error_code err, boost::shared_ptr<beacon_msg> beacon, boost::shared_ptr<beacon_ack> ack)
{
    failure_detector::on_beacon_ack(err, beacon, ack);

    zauto_lock l(_meta_lock);
    
    if (beacon->to == _current_meta_server)
    {
        if (err)
        {
            end_point node = find_next_meta_server(beacon->to);
            if (beacon->to != node)
            {
                switch_master(beacon->to, node);
            }
        }
        else if (ack->is_master == false)
        {
            if (end_point::INVALID != ack->primary_node)
            {
                switch_master(beacon->to, ack->primary_node);
            }
        }
    }

    else
    {
        if (err)
        {
            // nothing to do
        }
        else if (ack->is_master == false)
        {
            if (end_point::INVALID != ack->primary_node)
            {
                switch_master(beacon->to, ack->primary_node);
            }
        }
        else 
        {
            _current_meta_server = beacon->to;
        }
    }
}

// client side
void replication_failure_detector::on_master_disconnected( const std::vector<end_point>& nodes )
{
    bool primaryDisconnected = false;

    {
    zauto_lock l(_meta_lock);
    for (auto it = nodes.begin(); it != nodes.end(); it++)
    {
        if (_current_meta_server == *it)
            primaryDisconnected = true;
    }
    }

    if (primaryDisconnected)
    {
        _stub->on_meta_server_disconnected();
    }
}

void replication_failure_detector::on_master_connected( const end_point& node)
{
    bool isPrimary = false;

    {
    zauto_lock l(_meta_lock);
    isPrimary = (node == _current_meta_server);
    }

    if (isPrimary)
    {
        _stub->OnCoordinatorConnected();
    }
}

}} // end namespace

