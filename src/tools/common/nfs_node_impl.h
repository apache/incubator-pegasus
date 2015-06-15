# pragma once
# include <dsn/tool_api.h>
# include "nfs_client_impl.h"
# include "nfs_server_impl.h"
# include <dsn/internal/nfs.h>

namespace dsn { 
    namespace service {

		class nfs_node_impl : public nfs_node, public ::dsn::service::serverlet<nfs_node_impl>
        {
        public:
            nfs_node_impl(service_node* node)
				: nfs_node(node), serverlet<nfs_node_impl>("nfs.node")
            {
                _opts.init(system::config());
                _server = nullptr;

                // TODO: create timer to cleanup idle clients
            }

            virtual ~nfs_node_impl(void)
            {
                stop();
                {
                    zauto_lock l(_clients_lock);
                    _clients.clear();
                }
            }

	        virtual void call(std::shared_ptr<remote_copy_request> rci, aio_task_ptr& callback) override
	        {
                std::shared_ptr<nfs_client_impl> client = nullptr;
                {
                    zauto_lock l(_clients_lock);
                    auto it = _clients.find(rci->source);
                    if (it == _clients.end())
                    {
                        client.reset(new nfs_client_impl(rci->source, _opts));
                        _clients.insert(std::map<end_point, std::shared_ptr<nfs_client_impl> >::value_type(rci->source, client));
                    }
                    else
                    {
                        client = it->second;
                    }
                }

		        client->begin_remote_copy(rci, callback); // copy file request entry
	        }

            virtual error_code start() override
            {
                _server = new nfs_service_impl(_opts);
                _server->open_service();
                return ERR_SUCCESS;
            }

            virtual error_code stop() override
            {
                _server->close_service();
                delete _server;
                _server = nullptr;
                return ERR_SUCCESS;
            }

        private:
            nfs_opts         _opts;
	        nfs_service_impl *_server;

            zlock                                                  _clients_lock;
            std::map<end_point, std::shared_ptr<nfs_client_impl> > _clients;
        };
    } 
} 