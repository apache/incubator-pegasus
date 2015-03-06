#pragma once

# include <boost/asio.hpp>
# include <rdsn/internal/singleton.h>
# include <thread>
# include <memory>
# include <vector>
# include <rdsn/tool_api.h>

namespace rdsn {
    namespace tools {

        class shared_io_service : public utils::singleton<shared_io_service>
        {
        public:
            shared_io_service()
            {
                _io_service_worker_count = tool_app::config()->get_value<int>("rdsn.asio", "io_service_worker_count", 1);
                for (int i = 0; i < _io_service_worker_count; i++)
                {
                    _workers.push_back(std::shared_ptr<std::thread>(new std::thread([this]()
                    {
                        boost::asio::io_service::work work(ios);
                        ios.run();
                    })));
                }
            }

            boost::asio::io_service ios;

        private:
            int                                       _io_service_worker_count;
            std::vector<std::shared_ptr<std::thread>> _workers;
        };

    }
}