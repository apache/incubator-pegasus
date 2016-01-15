#!/bin/bash

set -e

tcp_network_providers="dsn::tools::sim_network_provider dsn::tools::asio_network_provider dsn::tools::hpc_network_provider"
udp_network_providers="dsn::tools::sim_network_provider dsn::tools::asio_udp_provider"
aio_providers="dsn::tools::empty_aio_provider dsn::tools::native_aio_provider"


mkdir -p perf-result
rm -rf data

#%replica_count% - how many replica servers we want in this test
for rep_cnt in {1..3};do
    #%tcp_network_provider% - what kind of tcp network providers we use
    for tcp in ${tcp_network_providers};do
        #%udp_network_provider% - what kind of udp network providers we use
        for udp in ${udp_network_providers};do
            #%aio_provider% - what kind of aio provider we use
            for aio in ${aio_providers};do
                ./dsn.replication.simple_kv perf-config.ini -cargs replica_count=${rep_cnt},tcp_network_provider=${tcp},udp_network_provider=${udp},aio_provider=${aio}
                cp data/client.perf.test/perf-result-* ./perf-result/
                rm -rf data
            done
        done
    done
done
