# pragma once
# include <dsn/tool_api.h>
# include "nfs_client_impl.h"
# include "nfs_server_impl.h"
# include <dsn/internal/nfs.h>


namespace dsn { 
    namespace service {

        class nfs_node_impl : public nfs_node, public virtual serverlet<nfs_node_impl>
        {
        public:
            nfs_node_impl(service_node* node)
                : nfs_node(node), serverlet<nfs_node_impl>("nfs.node")
            {
                _opts.init(system::config());
                _server = nullptr;
                _client = nullptr;
            }

            virtual ~nfs_node_impl(void)
            {
                stop();
            }

            virtual void call(std::shared_ptr<remote_copy_request> rci, aio_task_ptr& callback) override
            {
                _client->begin_remote_copy(rci, callback); // copy file request entry
            }

            virtual error_code start() override
            {
                _server = new nfs_service_impl(_opts);
                _server->open_service();

                _client = new nfs_client_impl(_opts);
                return ERR_SUCCESS;
            }

            virtual error_code stop() override
            {
                _server->close_service();
                delete _server;
                _server = nullptr;

                delete _client;
                _client = nullptr;

                return ERR_SUCCESS;
            }
    
        private:
            nfs_opts         _opts;
            nfs_service_impl *_server;
            nfs_client_impl  *_client;
        };
    } 
} 