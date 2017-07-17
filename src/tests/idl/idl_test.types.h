#pragma once
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/serialization.h>

#include "idl_test_types.h"
#include "idl_test.pb.h"

namespace dsn {
namespace idl {
namespace test {
GENERATED_TYPE_SERIALIZATION(test_thrift_item, THRIFT)
GENERATED_TYPE_SERIALIZATION(test_protobuf_item, PROTOBUF)
}
}
}