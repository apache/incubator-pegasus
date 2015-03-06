# pragma once

# include <rdsn/service_api.h>

namespace rdsn {
    namespace service {

        struct remote_copy_request
        {
            end_point   source;
            std::string source_dir;
            std::vector<std::string> files;
            std::string dest_dir;
            bool        overwrite;

        };

        struct remote_copy_response
        {
            
        };

        extern void marshall(::rdsn::utils::binary_writer& writer, const remote_copy_request& val, uint16_t pos = 0xffff);

        extern void unmarshall(::rdsn::utils::binary_reader& reader, __out remote_copy_request& val);

        class file_server 
        {

        };
    }
}