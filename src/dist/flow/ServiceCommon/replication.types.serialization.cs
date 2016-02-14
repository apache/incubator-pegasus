using System;
using System.IO;
using System.Collections.Generic;
using dsn.dev.csharp;

namespace dsn.replication 
{
    // ---------- partition_status -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out partition_status val)
        {
            UInt16 val2;
            rs.Read(out val2);
            val = (partition_status)val2;
        }

        public static void Write(this Stream ws, partition_status val)
        {
            ws.Write((UInt16)val);
        }
    }

    // ---------- read_semantic -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out read_semantic val)
        {
            UInt16 val2;
            rs.Read(out val2);
            val = (read_semantic)val2;
        }

        public static void Write(this Stream ws, read_semantic val)
        {
            ws.Write((UInt16)val);
        }
    }

    // ---------- learn_type -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out learn_type val)
        {
            UInt16 val2;
            rs.Read(out val2);
            val = (learn_type)val2;
        }

        public static void Write(this Stream ws, learn_type val)
        {
            ws.Write((UInt16)val);
        }
    }

    // ---------- learner_status -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out learner_status val)
        {
            UInt16 val2;
            rs.Read(out val2);
            val = (learner_status)val2;
        }

        public static void Write(this Stream ws, learner_status val)
        {
            ws.Write((UInt16)val);
        }
    }

    // ---------- config_type -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out config_type val)
        {
            UInt16 val2;
            rs.Read(out val2);
            val = (config_type)val2;
        }

        public static void Write(this Stream ws, config_type val)
        {
            ws.Write((UInt16)val);
        }
    }

    // ---------- app_status -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out app_status val)
        {
            UInt16 val2;
            rs.Read(out val2);
            val = (app_status)val2;
        }

        public static void Write(this Stream ws, app_status val)
        {
            ws.Write((UInt16)val);
        }
    }

    // ---------- node_status -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out node_status val)
        {
            UInt16 val2;
            rs.Read(out val2);
            val = (node_status)val2;
        }

        public static void Write(this Stream ws, node_status val)
        {
            ws.Write((UInt16)val);
        }
    }

    // ---------- global_partition_id -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out global_partition_id val)
        {
            val = new global_partition_id();
            rs.Read(out val.app_id);
            rs.Read(out val.pidx);
        }

        public static void Write(this Stream ws, global_partition_id val)
        {
            ws.Write(val.app_id);
            ws.Write(val.pidx);
        }
    }

    // ---------- mutation_header -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out mutation_header val)
        {
            val = new mutation_header();
            rs.Read(out val.gpid);
            rs.Read(out val.ballot);
            rs.Read(out val.decree);
            rs.Read(out val.log_offset);
            rs.Read(out val.last_committed_decree);
        }

        public static void Write(this Stream ws, mutation_header val)
        {
            ws.Write(val.gpid);
            ws.Write(val.ballot);
            ws.Write(val.decree);
            ws.Write(val.log_offset);
            ws.Write(val.last_committed_decree);
        }
    }

    // ---------- mutation_update -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out mutation_update val)
        {
            val = new mutation_update();
            rs.Read(out val.code);
            rs.Read(out val.data);
        }

        public static void Write(this Stream ws, mutation_update val)
        {
            ws.Write(val.code);
            ws.Write(val.data);
        }
    }

    // ---------- mutation_data -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out mutation_data val)
        {
            val = new mutation_data();
            rs.Read(out val.header);
            //rs.Read(out val.updates);
        }

        public static void Write(this Stream ws, mutation_data val)
        {
            ws.Write(val.header);
            //ws.Write(val.updates);
        }
    }

    // ---------- partition_configuration -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out partition_configuration val)
        {
            val = new partition_configuration();
            rs.Read(out val.app_type);
            rs.Read(out val.gpid);
            rs.Read(out val.ballot);
            rs.Read(out val.max_replica_count);
            rs.Read(out val.primary);
            rs.Read(out val.secondaries);
            rs.Read(out val.last_drops);
            rs.Read(out val.last_committed_decree);
        }

        public static void Write(this Stream ws, partition_configuration val)
        {
            ws.Write(val.app_type);
            ws.Write(val.gpid);
            ws.Write(val.ballot);
            ws.Write(val.max_replica_count);
            ws.Write(val.primary);
            ws.Write(val.secondaries);
            ws.Write(val.last_drops);
            ws.Write(val.last_committed_decree);
        }

        public static void Read(this Stream rs, out List<partition_configuration> val)
        {
            int c;
            rs.Read(out c);
            val = new List<partition_configuration>();
            for (int i = 0; i < c; i++)
            {
                partition_configuration v;
                rs.Read(out v);
                val.Add(v);
            }
        }

        public static void Write(this Stream ws, List<partition_configuration> val)
        {
            ws.Write((Int32)val.Count);
            foreach (var v in val)
            {
                ws.Write(v);
            }
        }
    }

    // ---------- replica_configuration -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out replica_configuration val)
        {
            val = new replica_configuration();
            rs.Read(out val.gpid);
            rs.Read(out val.ballot);
            rs.Read(out val.primary);
            rs.Read(out val.status);
            rs.Read(out val.learner_signature);
        }

        public static void Write(this Stream ws, replica_configuration val)
        {
            ws.Write(val.gpid);
            ws.Write(val.ballot);
            ws.Write(val.primary);
            ws.Write(val.status);
            ws.Write(val.learner_signature);
        }
    }

    // ---------- prepare_msg -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out prepare_msg val)
        {
            val = new prepare_msg();
            rs.Read(out val.config);
            rs.Read(out val.mu);
        }

        public static void Write(this Stream ws, prepare_msg val)
        {
            ws.Write(val.config);
            ws.Write(val.mu);
        }
    }

    // ---------- read_request_header -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out read_request_header val)
        {
            val = new read_request_header();
            rs.Read(out val.gpid);
            rs.Read(out val.code);
            rs.Read(out val.semantic);
            rs.Read(out val.version_decree);
        }

        public static void Write(this Stream ws, read_request_header val)
        {
            ws.Write(val.gpid);
            ws.Write(val.code);
            ws.Write(val.semantic);
            ws.Write(val.version_decree);
        }
    }

    // ---------- write_request_header -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out write_request_header val)
        {
            val = new write_request_header();
            rs.Read(out val.gpid);
            rs.Read(out val.code);
        }

        public static void Write(this Stream ws, write_request_header val)
        {
            ws.Write(val.gpid);
            ws.Write(val.code);
        }
    }

    // ---------- rw_response_header -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out rw_response_header val)
        {
            val = new rw_response_header();
            rs.Read(out val.err);
        }

        public static void Write(this Stream ws, rw_response_header val)
        {
            ws.Write(val.err);
        }
    }

    // ---------- prepare_ack -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out prepare_ack val)
        {
            val = new prepare_ack();
            rs.Read(out val.gpid);
            rs.Read(out val.err);
            rs.Read(out val.ballot);
            rs.Read(out val.decree);
            rs.Read(out val.last_committed_decree_in_app);
            rs.Read(out val.last_committed_decree_in_prepare_list);
        }

        public static void Write(this Stream ws, prepare_ack val)
        {
            ws.Write(val.gpid);
            ws.Write(val.err);
            ws.Write(val.ballot);
            ws.Write(val.decree);
            ws.Write(val.last_committed_decree_in_app);
            ws.Write(val.last_committed_decree_in_prepare_list);
        }
    }

    // ---------- learn_state -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out learn_state val)
        {
            val = new learn_state();
            rs.Read(out val.from_decree_excluded);
            rs.Read(out val.to_decree_included);
            //rs.Read(out val.meta);
            //rs.Read(out val.files);
        }

        public static void Write(this Stream ws, learn_state val)
        {
            ws.Write(val.from_decree_excluded);
            ws.Write(val.to_decree_included);
            //ws.Write(val.meta);
            //ws.Write(val.files);
        }
    }

    // ---------- learn_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out learn_request val)
        {
            val = new learn_request();
            rs.Read(out val.gpid);
            rs.Read(out val.learner);
            rs.Read(out val.signature);
            rs.Read(out val.last_committed_decree_in_app);
            rs.Read(out val.last_committed_decree_in_prepare_list);
            rs.Read(out val.app_specific_learn_request);
        }

        public static void Write(this Stream ws, learn_request val)
        {
            ws.Write(val.gpid);
            ws.Write(val.learner);
            ws.Write(val.signature);
            ws.Write(val.last_committed_decree_in_app);
            ws.Write(val.last_committed_decree_in_prepare_list);
            ws.Write(val.app_specific_learn_request);
        }
    }

    // ---------- learn_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out learn_response val)
        {
            val = new learn_response();
            rs.Read(out val.err);
            rs.Read(out val.config);
            rs.Read(out val.last_committed_decree);
            rs.Read(out val.prepare_start_decree);
            rs.Read(out val.type);
            rs.Read(out val.state);
            rs.Read(out val.address);
            rs.Read(out val.base_local_dir);
        }

        public static void Write(this Stream ws, learn_response val)
        {
            ws.Write(val.err);
            ws.Write(val.config);
            ws.Write(val.last_committed_decree);
            ws.Write(val.prepare_start_decree);
            ws.Write(val.type);
            ws.Write(val.state);
            ws.Write(val.address);
            ws.Write(val.base_local_dir);
        }
    }

    // ---------- group_check_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out group_check_request val)
        {
            val = new group_check_request();
            rs.Read(out val.app_type);
            rs.Read(out val.node);
            rs.Read(out val.config);
            rs.Read(out val.last_committed_decree);
        }

        public static void Write(this Stream ws, group_check_request val)
        {
            ws.Write(val.app_type);
            ws.Write(val.node);
            ws.Write(val.config);
            ws.Write(val.last_committed_decree);
        }
    }

    // ---------- group_check_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out group_check_response val)
        {
            val = new group_check_response();
            rs.Read(out val.gpid);
            rs.Read(out val.err);
            rs.Read(out val.last_committed_decree_in_app);
            rs.Read(out val.last_committed_decree_in_prepare_list);
            rs.Read(out val.learner_status_);
            rs.Read(out val.learner_signature);
            rs.Read(out val.node);
        }

        public static void Write(this Stream ws, group_check_response val)
        {
            ws.Write(val.gpid);
            ws.Write(val.err);
            ws.Write(val.last_committed_decree_in_app);
            ws.Write(val.last_committed_decree_in_prepare_list);
            ws.Write(val.learner_status_);
            ws.Write(val.learner_signature);
            ws.Write(val.node);
        }
    }

    // ---------- app_info -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out app_info val)
        {
            val = new app_info();
            rs.Read(out val.status);
            rs.Read(out val.app_type);
            rs.Read(out val.app_name);
            rs.Read(out val.app_id);
            rs.Read(out val.partition_count);
        }

        public static void Write(this Stream ws, app_info val)
        {
            ws.Write(val.status);
            ws.Write(val.app_type);
            ws.Write(val.app_name);
            ws.Write(val.app_id);
            ws.Write(val.partition_count);
        }
    }

    // ---------- node_info -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out node_info val)
        {
            val = new node_info();
            rs.Read(out val.status);
            rs.Read(out val.address);
        }

        public static void Write(this Stream ws, node_info val)
        {
            ws.Write(val.status);
            ws.Write(val.address);
        }
    }

    // ---------- meta_response_header -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out meta_response_header val)
        {
            val = new meta_response_header();
            rs.Read(out val.err);
            rs.Read(out val.primary_address);
        }

        public static void Write(this Stream ws, meta_response_header val)
        {
            ws.Write(val.err);
            ws.Write(val.primary_address);
        }
    }

    // ---------- configuration_update_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_update_request val)
        {
            val = new configuration_update_request();
            rs.Read(out val.config);
            rs.Read(out val.type);
            rs.Read(out val.node);
            rs.Read(out val.is_stateful);
        }

        public static void Write(this Stream ws, configuration_update_request val)
        {
            ws.Write(val.config);
            ws.Write(val.type);
            ws.Write(val.node);
            ws.Write(val.is_stateful);
        }
    }

    // ---------- configuration_update_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_update_response val)
        {
            val = new configuration_update_response();
            rs.Read(out val.err);
            rs.Read(out val.config);
        }

        public static void Write(this Stream ws, configuration_update_response val)
        {
            ws.Write(val.err);
            ws.Write(val.config);
        }
    }

    // ---------- configuration_query_by_node_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_query_by_node_request val)
        {
            val = new configuration_query_by_node_request();
            rs.Read(out val.node);
        }

        public static void Write(this Stream ws, configuration_query_by_node_request val)
        {
            ws.Write(val.node);
        }
    }

    // ---------- create_app_options -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out create_app_options val)
        {
            val = new create_app_options();
            rs.Read(out val.partition_count);
            rs.Read(out val.replica_count);
            rs.Read(out val.success_if_exist);
            rs.Read(out val.app_type);
            rs.Read(out val.is_stateful);
        }

        public static void Write(this Stream ws, create_app_options val)
        {
            ws.Write(val.partition_count);
            ws.Write(val.replica_count);
            ws.Write(val.success_if_exist);
            ws.Write(val.app_type);
            ws.Write(val.is_stateful);
        }
    }

    // ---------- configuration_create_app_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_create_app_request val)
        {
            val = new configuration_create_app_request();
            rs.Read(out val.app_name);
            rs.Read(out val.options);
        }

        public static void Write(this Stream ws, configuration_create_app_request val)
        {
            ws.Write(val.app_name);
            ws.Write(val.options);
        }
    }

    // ---------- drop_app_options -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out drop_app_options val)
        {
            val = new drop_app_options();
            rs.Read(out val.success_if_not_exist);
        }

        public static void Write(this Stream ws, drop_app_options val)
        {
            ws.Write(val.success_if_not_exist);
        }
    }

    // ---------- configuration_drop_app_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_drop_app_request val)
        {
            val = new configuration_drop_app_request();
            rs.Read(out val.app_name);
            rs.Read(out val.options);
        }

        public static void Write(this Stream ws, configuration_drop_app_request val)
        {
            ws.Write(val.app_name);
            ws.Write(val.options);
        }
    }

    // ---------- configuration_list_apps_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_list_apps_request val)
        {
            val = new configuration_list_apps_request();
            rs.Read(out val.status);
        }

        public static void Write(this Stream ws, configuration_list_apps_request val)
        {
            ws.Write(val.status);
        }
    }

    // ---------- configuration_list_nodes_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_list_nodes_request val)
        {
            val = new configuration_list_nodes_request();
            rs.Read(out val.status);
        }

        public static void Write(this Stream ws, configuration_list_nodes_request val)
        {
            ws.Write(val.status);
        }
    }

    // ---------- configuration_create_app_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_create_app_response val)
        {
            val = new configuration_create_app_response();
            rs.Read(out val.err);
            rs.Read(out val.appid);
        }

        public static void Write(this Stream ws, configuration_create_app_response val)
        {
            ws.Write(val.err);
            ws.Write(val.appid);
        }
    }

    // ---------- configuration_drop_app_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_drop_app_response val)
        {
            val = new configuration_drop_app_response();
            rs.Read(out val.err);
        }

        public static void Write(this Stream ws, configuration_drop_app_response val)
        {
            ws.Write(val.err);
        }
    }

    // ---------- configuration_list_apps_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_list_apps_response val)
        {
            val = new configuration_list_apps_response();
            rs.Read(out val.err);
            //rs.Read(out val.infos);
        }

        public static void Write(this Stream ws, configuration_list_apps_response val)
        {
            ws.Write(val.err);
            //ws.Write(val.infos);
        }
    }

    // ---------- configuration_list_nodes_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_list_nodes_response val)
        {
            val = new configuration_list_nodes_response();
            rs.Read(out val.err);
            //rs.Read(out val.infos);
        }

        public static void Write(this Stream ws, configuration_list_nodes_response val)
        {
            ws.Write(val.err);
            //ws.Write(val.infos);
        }
    }

    // ---------- configuration_query_by_node_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_query_by_node_response val)
        {
            val = new configuration_query_by_node_response();
            rs.Read(out val.err);
            rs.Read(out val.partitions);
        }

        public static void Write(this Stream ws, configuration_query_by_node_response val)
        {
            ws.Write(val.err);
            ws.Write(val.partitions);
        }
    }

    // ---------- configuration_query_by_index_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_query_by_index_request val)
        {
            val = new configuration_query_by_index_request();
            rs.Read(out val.app_name);
            rs.Read(out val.partition_indices);
        }

        public static void Write(this Stream ws, configuration_query_by_index_request val)
        {
            ws.Write(val.app_name);
            ws.Write(val.partition_indices);
        }
    }

    // ---------- configuration_query_by_index_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out configuration_query_by_index_response val)
        {
            val = new configuration_query_by_index_response();
            rs.Read(out val.err);
            rs.Read(out val.app_id);
            rs.Read(out val.partition_count);
            rs.Read(out val.partitions);
        }

        public static void Write(this Stream ws, configuration_query_by_index_response val)
        {
            ws.Write(val.err);
            ws.Write(val.app_id);
            ws.Write(val.partition_count);
            ws.Write(val.partitions);
        }
    }

    // ---------- query_replica_decree_request -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out query_replica_decree_request val)
        {
            val = new query_replica_decree_request();
            rs.Read(out val.gpid);
            rs.Read(out val.node);
        }

        public static void Write(this Stream ws, query_replica_decree_request val)
        {
            ws.Write(val.gpid);
            ws.Write(val.node);
        }
    }

    // ---------- query_replica_decree_response -------------
    public static partial class replicationHelper
    {
        public static void Read(this Stream rs, out query_replica_decree_response val)
        {
            val = new query_replica_decree_response();
            rs.Read(out val.err);
            rs.Read(out val.last_decree);
        }

        public static void Write(this Stream ws, query_replica_decree_response val)
        {
            ws.Write(val.err);
            ws.Write(val.last_decree);
        }
    }

} 
