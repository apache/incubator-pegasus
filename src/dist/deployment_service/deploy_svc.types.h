# pragma once
# include <dsn/service_api_cpp.h>
# include <dsn/cpp/serialization_helper/thrift_helper.h>

# include "deploy_svc_types.h"


namespace dsn {
    namespace dist {
        GENERATED_TYPE_SERIALIZATION(deploy_request)
        GENERATED_TYPE_SERIALIZATION(deploy_info)
        GENERATED_TYPE_SERIALIZATION(deploy_info_list)
        GENERATED_TYPE_SERIALIZATION(cluster_info)
        GENERATED_TYPE_SERIALIZATION(cluster_list)
    }
}