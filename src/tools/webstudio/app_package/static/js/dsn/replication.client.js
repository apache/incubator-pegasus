meta_sApp = function(website) {
    this.url = website;
}

meta_sApp.prototype = {};

meta_sApp.prototype.marshall = function(value, type) {
    return marshall_thrift_json(value, type);
}

meta_sApp.prototype.unmarshall = function(buf, value, type) {
    return unmarshall_thrift_json(buf, value, type);
}

meta_sApp.prototype.internal_create_app = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_CREATE_APP",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_create_app_response();
            self.unmarshall(result, ret, "struct");
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

meta_sApp.prototype.internal_async_create_app = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_CREATE_APP",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_create_app_response();
            self.unmarshall(result, ret, "struct");
            on_success(ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
            if (on_fail) {
                on_fail(xhr, textStatus, errorThrown);
            }
        }
    );
    return ret;
}

meta_sApp.prototype.create_app = function(obj) {
    if (!obj.async) {
        return this.internal_create_app(obj.args, obj.hash);
    } else {
        this.internal_async_create_app(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

meta_sApp.prototype.internal_drop_app = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_DROP_APP",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_drop_app_response();
            self.unmarshall(result, ret, "struct");
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

meta_sApp.prototype.internal_async_drop_app = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_DROP_APP",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_drop_app_response();
            self.unmarshall(result, ret, "struct");
            on_success(ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
            if (on_fail) {
                on_fail(xhr, textStatus, errorThrown);
            }
        }
    );
    return ret;
}

meta_sApp.prototype.drop_app = function(obj) {
    if (!obj.async) {
        return this.internal_drop_app(obj.args, obj.hash);
    } else {
        this.internal_async_drop_app(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

meta_sApp.prototype.internal_list_nodes = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_LIST_NODES",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_list_nodes_response();
            self.unmarshall(result, ret, "struct");
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

meta_sApp.prototype.internal_async_list_nodes = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_LIST_NODES",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_list_nodes_response();
            self.unmarshall(result, ret, "struct");
            on_success(ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
            if (on_fail) {
                on_fail(xhr, textStatus, errorThrown);
            }
        }
    );
    return ret;
}

meta_sApp.prototype.list_nodes = function(obj) {
    if (!obj.async) {
        return this.internal_list_nodes(obj.args, obj.hash);
    } else {
        this.internal_async_list_nodes(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

meta_sApp.prototype.internal_list_apps = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_LIST_APPS",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_list_apps_response();
            self.unmarshall(result, ret, "struct");
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

meta_sApp.prototype.internal_async_list_apps = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_LIST_APPS",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_list_apps_response();
            self.unmarshall(result, ret, "struct");
            on_success(ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
            if (on_fail) {
                on_fail(xhr, textStatus, errorThrown);
            }
        }
    );
    return ret;
}

meta_sApp.prototype.list_apps = function(obj) {
    if (!obj.async) {
        return this.internal_list_apps(obj.args, obj.hash);
    } else {
        this.internal_async_list_apps(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

meta_sApp.prototype.internal_query_configuration_by_node = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_QUERY_NODE_PARTITIONS",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_query_by_node_response();
            self.unmarshall(result, ret, "struct");
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

meta_sApp.prototype.internal_async_query_configuration_by_node = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_QUERY_NODE_PARTITIONS",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_query_by_node_response();
            self.unmarshall(result, ret, "struct");
            on_success(ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
            if (on_fail) {
                on_fail(xhr, textStatus, errorThrown);
            }
        }
    );
    return ret;
}

meta_sApp.prototype.query_configuration_by_node = function(obj) {
    if (!obj.async) {
        return this.internal_query_configuration_by_node(obj.args, obj.hash);
    } else {
        this.internal_async_query_configuration_by_node(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

meta_sApp.prototype.internal_query_configuration_by_index = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_query_by_index_response();
            self.unmarshall(result, ret, "struct");
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

meta_sApp.prototype.internal_async_query_configuration_by_index = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.url,
        "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX",
        hash,
        "POST",
        this.marshall(args, "struct"),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_query_by_index_response();
            self.unmarshall(result, ret, "struct");
            on_success(ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
            if (on_fail) {
                on_fail(xhr, textStatus, errorThrown);
            }
        }
    );
    return ret;
}

meta_sApp.prototype.query_configuration_by_index = function(obj) {
    if (!obj.async) {
        return this.internal_query_configuration_by_index(obj.args, obj.hash);
    } else {
        this.internal_async_query_configuration_by_index(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

