#pragma once
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/serialization.h>


#include "simple_kv_types.h"


namespace dsn { namespace replication { namespace application { 
    GENERATED_TYPE_SERIALIZATION(kv_pair, THRIFT)

} } } 