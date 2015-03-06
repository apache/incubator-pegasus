#pragma once

#include <rdsn/servicelet.h>

namespace rdsn { namespace service {

rpc_response_task_ptr rpc_replicated(
            const end_point& localAddr,
            const end_point& firstTryServer,
            const std::vector<end_point>& servers, 
            message_ptr& request,

            // reply
            service_base* svc,
            rpc_reply_handler callback, 
            int reply_hash);

rpc_response_task_ptr rpc_replicated(
        const end_point& localAddr,
        const end_point& firstTryServer,
        const std::vector<end_point>& servers, 
        message_ptr& request,

        // reply
        service_base* svc,
        rpc_reply_handler callback
        );

}} // end namespace
