#pragma once
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/serialization.h>


#include "deploy_svc_types.h"


namespace dsn { namespace dist { 
    GENERATED_TYPE_SERIALIZATION(deploy_request, THRIFT)
    GENERATED_TYPE_SERIALIZATION(deploy_info, THRIFT)
    GENERATED_TYPE_SERIALIZATION(deploy_info_list, THRIFT)
    GENERATED_TYPE_SERIALIZATION(cluster_info, THRIFT)
    GENERATED_TYPE_SERIALIZATION(cluster_list, THRIFT)

} } 