# pragma once
# include "counter.client.h"
# include "counter.client.perf.h"
# include "counter.server.h"
# include "counter.server.impl.h"
# include "test.common.h"

namespace dsn {
    namespace example {
        // server app example
        class counter_server_app :
            public ::dsn::service_app
        {
        public:
            counter_server_app() {}

            virtual ::dsn::error_code start(int argc, char** argv)
            {
                _counter_svc.open_service();
                return ::dsn::ERR_OK;
            }

            virtual void stop(bool cleanup = false)
            {
                _counter_svc.close_service();
            }

        private:
            counter_service_impl _counter_svc;
        };

        // client app example
        class counter_client_app :
            public ::dsn::service_app,
            public virtual ::dsn::clientlet
        {
        public:
            counter_client_app() {}

            ~counter_client_app()
            {
                stop();
            }

            virtual ::dsn::error_code start(int argc, char** argv)
            {
                if (argc < 2)
                {
                    printf("Usage: <exe> server-host server-port or service-url\n");
                    return ::dsn::ERR_INVALID_PARAMETERS;
                }

                if (argc == 2)
                {
                    // mem leak for uri-build, but we don't care here as it is once only
                    _server.assign_uri(dsn_uri_build(argv[1]));
                }
                else
                {
                    _server.assign_ipv4(argv[1], (uint16_t)atoi(argv[2]));
                }

                _counter_client.reset(new counter_client(_server));
                _timer = ::dsn::tasking::enqueue_timer(LPC_COUNTER_TEST_TIMER, this, [this] {on_test_timer(); }, std::chrono::seconds(1));
                return ::dsn::ERR_OK;
            }

            virtual void stop(bool cleanup = false)
            {
                _timer->cancel(true);

                _counter_client.reset();
            }

            virtual void on_test_timer()
            {
                if (witness == NULL)
                {
                    witness = new counter_test_helper(4);
                }
                // test for service 'counter'
                {
                    //sync:
                    counter_add_args args;
                    args.op.operand = TEST_ADD_ARGS_OPERAND;
                    args.op.name = TEST_ADD_ARGS_NAME;
                    auto result = _counter_client->add_sync(args);
                    if (result.first == ::dsn::ERR_OK)
                    {
                        bool ok = result.second.success == TEST_ADD_RESULT;
                        witness->add_result("client test add", ok);
                    }
                    //async: 
                    //_counter_client->add({});

                }
                {
                    //sync:
                    counter_read_args args;
                    args.name = TEST_READ_ARGS_NAME;
                    auto result = _counter_client->read_sync(args);
                    bool ok = result.first == ::dsn::ERR_OK && result.second.success == TEST_READ_RESULT;
                    witness->add_result("client test read", ok);
                    //async: 
                    //_counter_client->read({});

                }
            }

        private:
            ::dsn::task_ptr _timer;
            ::dsn::rpc_address _server;

            std::unique_ptr<counter_client> _counter_client;
        };

        class counter_perf_test_client_app :
            public ::dsn::service_app,
            public virtual ::dsn::clientlet
        {
        public:
            counter_perf_test_client_app()
            {
                _counter_client = nullptr;
            }

            ~counter_perf_test_client_app()
            {
                stop();
            }

            virtual ::dsn::error_code start(int argc, char** argv)
            {
                if (argc < 2)
                    return ::dsn::ERR_INVALID_PARAMETERS;

                _server.assign_ipv4(argv[1], (uint16_t)atoi(argv[2]));

                _counter_client = new counter_perf_test_client(_server);
                _counter_client->start_test();
                return ::dsn::ERR_OK;
            }

            virtual void stop(bool cleanup = false)
            {
                if (_counter_client != nullptr)
                {
                    delete _counter_client;
                    _counter_client = nullptr;
                }
            }

        private:
            counter_perf_test_client *_counter_client;
            ::dsn::rpc_address _server;
        };

    }
}