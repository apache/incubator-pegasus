#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>

#include "meta_service.h"
#include "server_state.h"
#include "meta_service_test_app.h"

dsn_message_t create_corresponding_receive(dsn_message_t request_msg)
{
    return dsn_msg_copy(request_msg);

    /*dsn::message_ex* rpc_message = reinterpret_cast<dsn::message_ex*>(request_msg);
    std::vector<dsn::blob>& buffers = rpc_message->buffers;

    int total_length = rpc_message->body_size() + sizeof(dsn::message_header);
    std::shared_ptr<char> recv_buffer(new char[total_length], std::default_delete<char[]>() );
    char* ptr = recv_buffer.get();
    int i=0;
    for (dsn::blob& bb: buffers) {
        memcpy(ptr, bb.data(), bb.length());
        i+=bb.length();
        ptr+=bb.length();
    }
    dassert(i==total_length, "");

    dsn::message_ex* result = dsn::message_ex::create_receive_message( dsn::blob(recv_buffer, total_length) );
    return result;*/
}

class fake_receiver_meta_service: public dsn::replication::meta_service
{
public:
    fake_receiver_meta_service(): dsn::replication::meta_service() {}
    virtual ~fake_receiver_meta_service() {}
    virtual void reply_message(dsn_message_t request, dsn_message_t response) override
    {
        uint64_t ptr;
        dsn::unmarshall(request, ptr);
        reply_context* ctx = reinterpret_cast<reply_context*>(ptr);
        ctx->response = create_corresponding_receive(response);
        dsn_msg_add_ref(ctx->response);

        //release the response
        dsn_msg_add_ref(response);
        dsn_msg_release_ref(response);

        ctx->e.notify();
    }
};

#define fake_create_app(state, request_data) \
    fake_rpc_call(RPC_CM_CREATE_APP, \
        LPC_META_STATE_NORMAL, \
        state, \
        &dsn::replication::server_state::create_app, \
        request_data)

#define fake_drop_app(state, request_data) \
    fake_rpc_call(RPC_CM_DROP_APP, \
        LPC_META_STATE_NORMAL, \
        state, \
        &dsn::replication::server_state::drop_app, \
        request_data)

#define fake_wait_rpc(context, response_data) do {\
    context->e.wait();\
    ::dsn::unmarshall(context->response, response_data);\
    dsn_msg_release_ref(context->response);\
} while(0)

void meta_service_test_app::data_definition_op_test()
{
    std::shared_ptr<fake_receiver_meta_service> svc = std::make_shared<fake_receiver_meta_service>();
    dsn::error_code ec = svc->remote_storage_initialize();
    ASSERT_EQ(ec, dsn::ERR_OK);

    svc->_state->initialize(svc.get(), svc->_cluster_root+"/apps");
    ec = svc->_state->initialize_data_structure();
    ASSERT_EQ(ec, dsn::ERR_OK);

    svc->_started = true;

    std::shared_ptr<reply_context> result, result2, result3;
    dsn::replication::configuration_create_app_request create_request;
    dsn::replication::configuration_create_app_response create_response;

    //normal create app
    create_request.app_name = "test_app2";
    create_request.options.app_type = "simple_kv";
    create_request.options.partition_count = 10;
    create_request.options.replica_count = 3;
    create_request.options.success_if_exist = false;

    result = fake_create_app(svc->_state.get(), create_request);
    fake_wait_rpc(result, create_response);
    ASSERT_EQ(create_response.appid, 2);
    ASSERT_EQ(create_response.err, dsn::ERR_OK);
    //first wait the table to create
    ASSERT_TRUE(svc->_state->spin_wait_creating(30));

    //create a exist app, with success flags false
    result = fake_create_app(svc->_state.get(), create_request);
    fake_wait_rpc(result, create_response);
    ASSERT_TRUE(create_response.err==dsn::ERR_INVALID_PARAMETERS);

    //create same app with success flags true, but different params
    create_request.options.partition_count += 10;
    create_request.options.app_type = "rrdb";
    create_request.options.success_if_exist = true;
    result = fake_create_app(svc->_state.get(), create_request);
    fake_wait_rpc(result, create_response);
    ASSERT_EQ( create_response.err, dsn::ERR_INVALID_PARAMETERS);

    //drop current app
    dsn::replication::configuration_drop_app_request drop_request;
    dsn::replication::configuration_drop_app_response drop_response;
    drop_request.app_name = "test_app2";
    drop_request.options.success_if_not_exist = false;

    result = fake_drop_app(svc->_state.get(), drop_request);
    //a drop request immediately after the first request
    result2 = fake_drop_app(svc->_state.get(), drop_request);
    //try to create a app with same name when it is dropping
    result3 = fake_create_app(svc->_state.get(), create_request);

    result->e.wait();
    result2->e.wait();
    result3->e.wait();

    ::dsn::unmarshall(result->response, drop_response);
    dsn_msg_release_ref(result->response);
    ASSERT_EQ(drop_response.err, dsn::ERR_OK);

    ::dsn::unmarshall(result2->response, drop_response);
    dsn_msg_release_ref(result2->response);
    ASSERT_TRUE(drop_response.err == dsn::ERR_APP_NOT_EXIST || drop_response.err == dsn::ERR_BUSY_DROPPING);

    ::dsn::unmarshall(result3->response, create_response);
    dsn_msg_release_ref(result3->response);
    ASSERT_TRUE(create_response.err==dsn::ERR_BUSY_DROPPING || create_response.err == dsn::ERR_OK);

    int created_successfully = 2;
    //in case that the previous create request succeed
    if (create_response.err == dsn::ERR_OK)
    {
        created_successfully = 3;
        drop_request.options.success_if_not_exist = true;
        do {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            result = fake_drop_app(svc->_state.get(), drop_request);
            fake_wait_rpc(result, drop_response);
        } while (drop_response.err != dsn::ERR_OK);
    }

    //currently "test_app2" is dropped, let's drop it again
    drop_request.options.success_if_not_exist = false;
    result = fake_drop_app(svc->_state.get(), drop_request);
    fake_wait_rpc(result, drop_response);
    ASSERT_EQ(dsn::ERR_APP_NOT_EXIST, drop_response.err);

    //now let's recreating the app with the same name
    result = fake_create_app(svc->_state.get(), create_request);
    create_request.options.success_if_exist = true;
    result2 = fake_create_app(svc->_state.get(), create_request);

    result->e.wait();
    result2->e.wait();

    ::dsn::unmarshall(result->response, create_response);
    dsn_msg_release_ref(result->response);
    ASSERT_EQ(create_response.err, dsn::ERR_OK);
    ASSERT_EQ(create_response.appid, created_successfully+1);

    ::dsn::unmarshall(result2->response, create_response);
    dsn_msg_release_ref(result2->response);
    ASSERT_EQ(create_response.err, dsn::ERR_BUSY_CREATING);

    ASSERT_TRUE(svc->_state->spin_wait_creating(30));

    //finally list the apps
    dsn::replication::configuration_list_apps_request list_request;
    dsn::replication::configuration_list_apps_response list_response;

    list_request.status = dsn::app_status::AS_AVAILABLE;
    svc->_state->list_apps(list_request, list_response);
    ASSERT_EQ(list_response.err, dsn::ERR_OK);
    ASSERT_EQ(list_response.infos.size(), 2);

    for (dsn::app_info& info: list_response.infos) {
        if (info.app_id == created_successfully+1) {
            ASSERT_EQ(info.app_name, create_request.app_name);
            ASSERT_EQ(info.app_type, create_request.options.app_type);
            ASSERT_EQ(info.partition_count, create_request.options.partition_count);
            ASSERT_EQ(info.status, dsn::app_status::AS_AVAILABLE);
        }
    }
}
