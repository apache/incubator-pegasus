#pragma once
#include <dsn/cpp/serialization_helper/dsn.layer2_types.h>
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/serialization.h>




namespace dsn { 
    GENERATED_TYPE_SERIALIZATION(partition_configuration, THRIFT)
    GENERATED_TYPE_SERIALIZATION(configuration_query_by_index_request, THRIFT)
    GENERATED_TYPE_SERIALIZATION(configuration_query_by_index_response, THRIFT)
    GENERATED_TYPE_SERIALIZATION(app_info, THRIFT)

} 