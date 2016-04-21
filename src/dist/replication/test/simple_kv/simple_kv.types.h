# pragma once
# include <dsn/service_api_cpp.h>

# include "simple_kv_types.h"

namespace dsn {
    namespace replication {
        namespace test {
            GENERATED_TYPE_SERIALIZATION(kv_pair, THRIFT)
        }
    }
}