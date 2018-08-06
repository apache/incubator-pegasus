#pragma once
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/serialization.h>

#include "nfs_types.h"

namespace dsn {
namespace service {
GENERATED_TYPE_SERIALIZATION(copy_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(copy_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(get_file_size_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(get_file_size_response, THRIFT)
}
}
