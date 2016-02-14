using System;
using System.IO;
using dsn.dev.csharp;

namespace dsn.replication 
{
    public class meta_sClient : Clientlet
    {
        private RpcAddress _server;
        
        public meta_sClient(RpcAddress server) { _server = server; }
        public meta_sClient() { }
        ~meta_sClient() {}

    
        // ---------- call replicationHelper.RPC_REPLICATION_META_S_CREATE_APP ------------
        // - synchronous 
        public ErrorCode create_app(
            configuration_create_app_request req, 
            out configuration_create_app_response resp, 
            int timeout_milliseconds = 0, 
            int hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_CREATE_APP, timeout_milliseconds, hash);
            s.Write(req);
            s.Flush();
            
            var respStream = RpcCallSync(server != null ? server : _server, s);
            if (null == respStream)
            {
                resp = default(configuration_create_app_response);
                return ErrorCode.ERR_TIMEOUT;
            }
            else
            {
                respStream.Read(out resp);
                return ErrorCode.ERR_OK;
            }
        }
        
        // - asynchronous with on-stack configuration_create_app_request and configuration_create_app_response 
        public delegate void create_appCallback(ErrorCode err, configuration_create_app_response resp);
        public void create_app(
            configuration_create_app_request req, 
            create_appCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_CREATE_APP,timeout_milliseconds, request_hash);
            s.Write(req);
            s.Flush();
            
            RpcCallAsync(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_create_app_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }        
        
        public SafeTaskHandle create_app2(
            configuration_create_app_request req, 
            create_appCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_CREATE_APP,timeout_milliseconds, request_hash);
            s.Write(req);
            s.Flush();
            
            return RpcCallAsync2(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_create_app_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }       

        // ---------- call replicationHelper.RPC_REPLICATION_META_S_DROP_APP ------------
        // - synchronous 
        public ErrorCode drop_app(
            configuration_drop_app_request req, 
            out configuration_drop_app_response resp, 
            int timeout_milliseconds = 0, 
            int hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_DROP_APP, timeout_milliseconds, hash);
            s.Write(req);
            s.Flush();
            
            var respStream = RpcCallSync(server != null ? server : _server, s);
            if (null == respStream)
            {
                resp = default(configuration_drop_app_response);
                return ErrorCode.ERR_TIMEOUT;
            }
            else
            {
                respStream.Read(out resp);
                return ErrorCode.ERR_OK;
            }
        }
        
        // - asynchronous with on-stack configuration_drop_app_request and configuration_drop_app_response 
        public delegate void drop_appCallback(ErrorCode err, configuration_drop_app_response resp);
        public void drop_app(
            configuration_drop_app_request req, 
            drop_appCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_DROP_APP,timeout_milliseconds, request_hash);
            s.Write(req);
            s.Flush();
            
            RpcCallAsync(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_drop_app_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }        
        
        public SafeTaskHandle drop_app2(
            configuration_drop_app_request req, 
            drop_appCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_DROP_APP,timeout_milliseconds, request_hash);
            s.Write(req);
            s.Flush();
            
            return RpcCallAsync2(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_drop_app_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }       

        // ---------- call replicationHelper.RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_NODE ------------
        // - synchronous 
        public ErrorCode query_configuration_by_node(
            configuration_query_by_node_request query, 
            out configuration_query_by_node_response resp, 
            int timeout_milliseconds = 0, 
            int hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_NODE, timeout_milliseconds, hash);
            s.Write(query);
            s.Flush();
            
            var respStream = RpcCallSync(server != null ? server : _server, s);
            if (null == respStream)
            {
                resp = default(configuration_query_by_node_response);
                return ErrorCode.ERR_TIMEOUT;
            }
            else
            {
                respStream.Read(out resp);
                return ErrorCode.ERR_OK;
            }
        }
        
        // - asynchronous with on-stack configuration_query_by_node_request and configuration_query_by_node_response 
        public delegate void query_configuration_by_nodeCallback(ErrorCode err, configuration_query_by_node_response resp);
        public void query_configuration_by_node(
            configuration_query_by_node_request query, 
            query_configuration_by_nodeCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_NODE,timeout_milliseconds, request_hash);
            s.Write(query);
            s.Flush();
            
            RpcCallAsync(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_query_by_node_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }        
        
        public SafeTaskHandle query_configuration_by_node2(
            configuration_query_by_node_request query, 
            query_configuration_by_nodeCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_NODE,timeout_milliseconds, request_hash);
            s.Write(query);
            s.Flush();
            
            return RpcCallAsync2(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_query_by_node_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }       

        // ---------- call replicationHelper.RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_INDEX ------------
        // - synchronous 
        public ErrorCode query_configuration_by_index(
            configuration_query_by_index_request query, 
            out configuration_query_by_index_response resp, 
            int timeout_milliseconds = 0, 
            int hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_INDEX, timeout_milliseconds, hash);
            s.Write(query);
            s.Flush();
            
            var respStream = RpcCallSync(server != null ? server : _server, s);
            if (null == respStream)
            {
                resp = default(configuration_query_by_index_response);
                return ErrorCode.ERR_TIMEOUT;
            }
            else
            {
                respStream.Read(out resp);
                return ErrorCode.ERR_OK;
            }
        }
        
        // - asynchronous with on-stack configuration_query_by_index_request and configuration_query_by_index_response 
        public delegate void query_configuration_by_indexCallback(ErrorCode err, configuration_query_by_index_response resp);
        public void query_configuration_by_index(
            configuration_query_by_index_request query, 
            query_configuration_by_indexCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_INDEX,timeout_milliseconds, request_hash);
            s.Write(query);
            s.Flush();
            
            RpcCallAsync(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_query_by_index_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }        
        
        public SafeTaskHandle query_configuration_by_index2(
            configuration_query_by_index_request query, 
            query_configuration_by_indexCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_INDEX,timeout_milliseconds, request_hash);
            s.Write(query);
            s.Flush();
            
            return RpcCallAsync2(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_query_by_index_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }       

        // ---------- call replicationHelper.RPC_REPLICATION_META_S_UPDATE_CONFIGURATION ------------
        // - synchronous 
        public ErrorCode update_configuration(
            configuration_update_request update, 
            out configuration_update_response resp, 
            int timeout_milliseconds = 0, 
            int hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_UPDATE_CONFIGURATION, timeout_milliseconds, hash);
            s.Write(update);
            s.Flush();
            
            var respStream = RpcCallSync(server != null ? server : _server, s);
            if (null == respStream)
            {
                resp = default(configuration_update_response);
                return ErrorCode.ERR_TIMEOUT;
            }
            else
            {
                respStream.Read(out resp);
                return ErrorCode.ERR_OK;
            }
        }
        
        // - asynchronous with on-stack configuration_update_request and configuration_update_response 
        public delegate void update_configurationCallback(ErrorCode err, configuration_update_response resp);
        public void update_configuration(
            configuration_update_request update, 
            update_configurationCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_UPDATE_CONFIGURATION,timeout_milliseconds, request_hash);
            s.Write(update);
            s.Flush();
            
            RpcCallAsync(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_update_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }        
        
        public SafeTaskHandle update_configuration2(
            configuration_update_request update, 
            update_configurationCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(replicationHelper.RPC_REPLICATION_META_S_UPDATE_CONFIGURATION,timeout_milliseconds, request_hash);
            s.Write(update);
            s.Flush();
            
            return RpcCallAsync2(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                configuration_update_response resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }       
    
    }

} // end namespace
