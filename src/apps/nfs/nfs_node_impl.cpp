
# include <dsn/tool/nfs_node_simple.h>
# include "nfs_client_impl.h"
# include "nfs_server_impl.h"

namespace dsn { 
    namespace service {

        nfs_node_simple::nfs_node_simple(::dsn::service_node* node)
            : nfs_node(node)
        {
            _opts = new nfs_opts();
            _opts->init();
            _server = nullptr;
            _client = nullptr;
        }

        nfs_node_simple::~nfs_node_simple(void)
        {
            stop();
            delete _opts;
        }

        void nfs_node_simple::call(std::shared_ptr<remote_copy_request> rci, aio_task* callback)
        {
            _client->begin_remote_copy(rci, callback); // copy file request entry
        }

        error_code nfs_node_simple::start()
        {
            _server = new nfs_service_impl(*_opts);
            _server->open_service();

            _client = new nfs_client_impl(*_opts);
            return ERR_OK;
        }

        error_code nfs_node_simple::stop()
        {
            _server->close_service();
            delete _server;
            _server = nullptr;

            delete _client;
            _client = nullptr;

            return ERR_OK;
        }
    } 
} 