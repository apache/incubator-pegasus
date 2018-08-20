#ifndef META_SERVICE_TEST_APP_H
#define META_SERVICE_TEST_APP_H

#include <dsn/service_api_cpp.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/dist/replication/replication_types.h>
#include <dsn/dist/replication/meta_service_app.h>
#include "dist/replication/meta_server/server_state.h"
#include "dist/replication/meta_server/meta_service.h"

class spin_counter
{
private:
    std::atomic_int _counter;

public:
    spin_counter() { _counter.store(0); }
    void wait()
    {
        while (_counter.load() != 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    void block() { ++_counter; }
    void notify() { --_counter; }
};

struct reply_context
{
    dsn::message_ex *response;
    spin_counter e;
};
dsn::message_ex *create_corresponding_receive(dsn::message_ex *req);

class fake_receiver_meta_service : public dsn::replication::meta_service
{
public:
    fake_receiver_meta_service() : meta_service() {}
    virtual ~fake_receiver_meta_service() {}
    virtual void reply_message(dsn::message_ex *request, dsn::message_ex *response) override
    {
        uint64_t ptr;
        dsn::unmarshall(request, ptr);
        reply_context *ctx = reinterpret_cast<reply_context *>(ptr);
        ctx->response = create_corresponding_receive(response);
        ctx->response->add_ref();

        // release the response
        response->add_ref();
        response->release_ref();

        ctx->e.notify();
    }
};

// release the dsn_message who's reference is 0
inline void destroy_message(dsn::message_ex *msg)
{
    msg->add_ref();
    msg->release_ref();
}

#define fake_wait_rpc(context, response_data)                                                      \
    do {                                                                                           \
        context->e.wait();                                                                         \
        unmarshall(context->response, response_data);                                              \
        context->response->release_ref();                                                          \
    } while (0)

class meta_service_test_app : public dsn::service_app
{
public:
    meta_service_test_app(const dsn::service_app_info *info) : service_app(info) {}

public:
    virtual dsn::error_code start(const std::vector<std::string> &args) override;
    virtual dsn::error_code stop(bool /*cleanup*/) { return dsn::ERR_OK; }
    void state_sync_test();
    void data_definition_op_test();
    void update_configuration_test();
    void balancer_validator();
    void balance_config_file();
    void apply_balancer_test();
    void cannot_run_balancer_test();
    void construct_apps_test();

    void simple_lb_cure_test();
    void simple_lb_balanced_cure();
    void simple_lb_from_proposal_test();
    void simple_lb_collect_replica();
    void simple_lb_construct_replica();
    void json_compacity();

    void policy_context_test();
    void backup_service_test();

    // test server_state set_app_envs/del_app_envs/clear_app_envs
    void app_envs_basic_test();

    // test for bug found
    void adjust_dropped_size();

    void call_update_configuration(
        dsn::replication::meta_service *svc,
        std::shared_ptr<dsn::replication::configuration_update_request> &request);
    void call_config_sync(
        dsn::replication::meta_service *svc,
        std::shared_ptr<dsn::replication::configuration_query_by_node_request> &request);

    template <typename TRequest, typename RequestHandler>
    std::shared_ptr<reply_context>
    fake_rpc_call(dsn::task_code rpc_code,
                  dsn::task_code server_state_write_code,
                  RequestHandler *handle_class,
                  void (RequestHandler::*handle)(dsn::message_ex *request),
                  const TRequest &data,
                  int hash = 0,
                  std::chrono::milliseconds delay = std::chrono::milliseconds(0))
    {
        dsn::message_ex *msg = dsn::message_ex::create_request(rpc_code);
        dsn::marshall(msg, data);

        std::shared_ptr<reply_context> result = std::make_shared<reply_context>();
        result->e.block();
        uint64_t ptr = reinterpret_cast<uint64_t>(result.get());
        dsn::marshall(msg, ptr);

        dsn::message_ex *received = create_corresponding_receive(msg);
        received->add_ref();
        dsn::tasking::enqueue(server_state_write_code,
                              nullptr,
                              std::bind(handle, handle_class, received),
                              hash,
                              delay);

        // release the sending message
        destroy_message(msg);

        return result;
    }

private:
    typedef std::function<bool(const dsn::replication::app_mapper &)> state_validator;
    bool
    wait_state(dsn::replication::server_state *ss, const state_validator &validator, int time = -1);
};

#endif // META_SERVICE_TEST_APP_H
