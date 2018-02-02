#pragma once
#include <dsn/tool/nfs/nfs_types.h>
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/serialization.h>




namespace dsn { namespace service { 
    GENERATED_TYPE_SERIALIZATION(copy_request, THRIFT)
    GENERATED_TYPE_SERIALIZATION(copy_response, THRIFT)
    GENERATED_TYPE_SERIALIZATION(get_file_size_request, THRIFT)
    GENERATED_TYPE_SERIALIZATION(get_file_size_response, THRIFT)

} } 