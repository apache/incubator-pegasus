# pragma once
# include <dsn/service_api_cpp.h>
# include <dsn/cpp/serialization.h>


# include <dsn/cpp/serialization_helper/dsn.layer2_types.h>


namespace dsn { 
    GENERATED_TYPE_SERIALIZATION(partition_configuration)
    GENERATED_TYPE_SERIALIZATION(configuration_query_by_index_request)
    GENERATED_TYPE_SERIALIZATION(configuration_query_by_index_response)
    GENERATED_TYPE_SERIALIZATION(app_info)

} 