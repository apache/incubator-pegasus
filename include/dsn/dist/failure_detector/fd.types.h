#pragma once
#include <dsn/dist/failure_detector/fd_types.h>
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/serialization.h>




namespace dsn { namespace fd { 
    GENERATED_TYPE_SERIALIZATION(beacon_msg, THRIFT)
    GENERATED_TYPE_SERIALIZATION(beacon_ack, THRIFT)
    GENERATED_TYPE_SERIALIZATION(config_master_message, THRIFT)

} } 