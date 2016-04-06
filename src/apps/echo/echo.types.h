# pragma once
# include <dsn/service_api_cpp.h>

//
// uncomment the following line if you want to use 
// data encoding/decoding from the original tool instead of rDSN
// in this case, you need to use these tools to generate
// type files with --gen=cpp etc. options
//

# if defined(DSN_USE_THRIFT_SERIALIZATION)

# include "echo_types.h"

# elif defined(DSN_USE_PROTO_SERIALIZATION)

# include "echo.pb.h"

# else // use rDSN's data encoding/decoding

namespace dsn { namespace example { 
} } 

#endif 
