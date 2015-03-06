#include "app_client_example1_service.h"
#include "replication_common.h"

app_client_example1_service::app_client_example1_service(service_app_spec* s, configuration_ptr c)
: service_app(s, c), serviceletex<app_client_example1_service>(s->name.c_str())
{
    _client = nullptr;
    _writeTimer = nullptr;
}

app_client_example1_service::~app_client_example1_service(void)
{

}

error_code app_client_example1_service::start(int argc, char** argv)
{
    replication_options opts;
    opts.initialize(config());

    _client = new app_client_example1(opts.MetaServers);
    _writeTimer = enqueue_task(LPC_TEST, &app_client_example1_service::OnTimer, 0, 1000, 1000);

    return ERR_SUCCESS;
}

void app_client_example1_service::stop(bool cleanup)
{
    if (_writeTimer != nullptr)
    {
        _writeTimer->cancel(true);
        _writeTimer = nullptr;
    }

    if (_client != nullptr)
    {
        delete _client;
        _client = nullptr;
    }
}


static void ResponseHandler(int err, message_ptr& request, message_ptr& response)
{

}

void app_client_example1_service::OnTimer()
{
    _client->append("TestKey", "TestValue", ResponseHandler);
}
