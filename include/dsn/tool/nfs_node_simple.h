# pragma once

# include <dsn/tool_api.h>
# include <dsn/internal/nfs.h>

namespace dsn { 
    namespace service {

        class nfs_service_impl;
        class nfs_client_impl;
        struct nfs_opts;
        class nfs_node_simple : public nfs_node
        {
        public:
            nfs_node_simple(::dsn::service_node* node);

            virtual ~nfs_node_simple(void);

            virtual void call(std::shared_ptr<remote_copy_request> rci, aio_task* callback) override;

            virtual ::dsn::error_code start() override;

            virtual error_code stop() override;
    
        private:
            nfs_opts         *_opts;
            nfs_service_impl *_server;
            nfs_client_impl  *_client;
        };
    } 
} 