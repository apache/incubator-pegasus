#ifndef META_SERVICE_TEST_APP_H
#define META_SERVICE_TEST_APP_H

#include <dsn/service_api_cpp.h>
#include <dsn/dist/replication/replication_types.h>
#include <dsn/dist/replication/meta_service_app.h>
#include "server_state.h"

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
    dsn_message_t response;
    spin_counter e;
};
dsn_message_t create_corresponding_receive(dsn_message_t req);

class meta_service_test_app: public dsn::service::meta_service_app
{
public:
    meta_service_test_app(dsn_gpid pid): dsn::service::meta_service_app(pid) {}

public:
    virtual dsn::error_code start(int, char **argv) override;
    virtual dsn::error_code stop(bool /*cleanup*/) { return dsn::ERR_OK; }
    void state_sync_test();
    void data_definition_op_test();
    void update_configuration_test();
    void balancer_validator();
    void apply_balancer_test();

    void call_update_configuration(dsn::replication::meta_service* svc,
        std::shared_ptr<dsn::replication::configuration_update_request>& request);

    template<typename TRequest>
    std::shared_ptr<reply_context> fake_rpc_call(
        dsn_task_code_t rpc_code,
        dsn_task_code_t server_state_write_code,
        dsn::replication::server_state* ss,
        void (dsn::replication::server_state::*handle)(dsn_message_t request),
        const TRequest& data,
        std::chrono::milliseconds delay = std::chrono::milliseconds(0))
    {
        dsn_message_t msg = dsn_msg_create_request(rpc_code);
        dsn::marshall(msg, data);

        std::shared_ptr<reply_context> result = std::make_shared<reply_context>();
        result->e.block();
        uint64_t ptr = reinterpret_cast<uint64_t>(result.get());
        dsn::marshall(msg, ptr);

        dsn_message_t received = create_corresponding_receive(msg);
        dsn_msg_add_ref(received);
        dsn::tasking::enqueue(
            server_state_write_code,
            nullptr,
            std::bind(handle, ss, received),
            dsn::replication::server_state::s_state_write_hash,
            delay
        );

        //release the sending message
        dsn_msg_add_ref(msg);
        dsn_msg_release_ref(msg);

        return result;
    }
private:
    typedef std::function<bool (const dsn::replication::app_mapper&)> state_validator;
    bool wait_state(
        dsn::replication::server_state* ss,
        const state_validator& validator,
        int time=-1);
};

#endif // META_SERVICE_TEST_APP_H
