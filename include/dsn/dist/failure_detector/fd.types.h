# pragma once
# include <dsn/service_api_cpp.h>

# include <dsn/dist/failure_detector/fd_types.h>
# include <dsn/dist/failure_detector/fd.code.definition.h>

# include <dsn/cpp/serialization_helper/thrift_helper.h>

namespace dsn {
    namespace fd {
        GENERATED_TYPE_SERIALIZATION(beacon_msg)
        GENERATED_TYPE_SERIALIZATION(beacon_ack)
        GENERATED_TYPE_SERIALIZATION(config_master_message)
    }
}