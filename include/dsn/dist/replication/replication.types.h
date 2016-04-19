# pragma once
# include <dsn/service_api_cpp.h>
# include <dsn/cpp/serialization.h>


# include <dsn/dist/replication/replication_types.h>


namespace dsn { namespace replication { 
    GENERATED_TYPE_SERIALIZATION(mutation_header)
    GENERATED_TYPE_SERIALIZATION(mutation_update)
    GENERATED_TYPE_SERIALIZATION(mutation_data)
    GENERATED_TYPE_SERIALIZATION(replica_configuration)
    GENERATED_TYPE_SERIALIZATION(prepare_msg)
    GENERATED_TYPE_SERIALIZATION(read_request_header)
    GENERATED_TYPE_SERIALIZATION(write_request_header)
    GENERATED_TYPE_SERIALIZATION(rw_response_header)
    GENERATED_TYPE_SERIALIZATION(prepare_ack)
    GENERATED_TYPE_SERIALIZATION(learn_state)
    GENERATED_TYPE_SERIALIZATION(learn_request)
    GENERATED_TYPE_SERIALIZATION(learn_response)
    GENERATED_TYPE_SERIALIZATION(group_check_request)
    GENERATED_TYPE_SERIALIZATION(group_check_response)
    GENERATED_TYPE_SERIALIZATION(node_info)
    GENERATED_TYPE_SERIALIZATION(meta_response_header)
    GENERATED_TYPE_SERIALIZATION(configuration_update_request)
    GENERATED_TYPE_SERIALIZATION(configuration_update_response)
    GENERATED_TYPE_SERIALIZATION(configuration_query_by_node_request)
    GENERATED_TYPE_SERIALIZATION(configuration_query_by_node_response)
    GENERATED_TYPE_SERIALIZATION(create_app_options)
    GENERATED_TYPE_SERIALIZATION(configuration_create_app_request)
    GENERATED_TYPE_SERIALIZATION(drop_app_options)
    GENERATED_TYPE_SERIALIZATION(configuration_drop_app_request)
    GENERATED_TYPE_SERIALIZATION(configuration_list_apps_request)
    GENERATED_TYPE_SERIALIZATION(configuration_list_nodes_request)
    GENERATED_TYPE_SERIALIZATION(configuration_create_app_response)
    GENERATED_TYPE_SERIALIZATION(control_balancer_migration_request)
    GENERATED_TYPE_SERIALIZATION(control_balancer_migration_response)
    GENERATED_TYPE_SERIALIZATION(balancer_proposal_request)
    GENERATED_TYPE_SERIALIZATION(balancer_proposal_response)
    GENERATED_TYPE_SERIALIZATION(configuration_drop_app_response)
    GENERATED_TYPE_SERIALIZATION(configuration_list_apps_response)
    GENERATED_TYPE_SERIALIZATION(configuration_list_nodes_response)
    GENERATED_TYPE_SERIALIZATION(query_replica_decree_request)
    GENERATED_TYPE_SERIALIZATION(query_replica_decree_response)
    GENERATED_TYPE_SERIALIZATION(replica_info)
    GENERATED_TYPE_SERIALIZATION(query_replica_info_request)
    GENERATED_TYPE_SERIALIZATION(query_replica_info_response)
    GENERATED_TYPE_SERIALIZATION(app_state)
    GENERATED_TYPE_SERIALIZATION(node_state)

} } 