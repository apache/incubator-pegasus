replica_sApp = function(website) {
    this.url = website;
}

replica_sApp.prototype = {};

replica_sApp.prototype.marshall_generated_type = function(value) {
    return marshall_thrift_json_generated_type(value);
}

replica_sApp.prototype.marshall_basic_type = function(value, type) {
    return marshall_thrift_json_basic_type(value, type);
}

replica_sApp.prototype.unmarshall_generated_type = function(buf, ret) {
    unmarshall_thrift_json_generated_type(buf, ret);
}

replica_sApp.prototype.unmarshall_basic_type = function(buf, type) {
    return unmarshall_thrift_json_basic_type(buf, type);
}

replica_sApp.prototype.get_address = function(url, hash) {
    if (typeof hash == "undefined") {
        hash = 0;
    }
    return url + "/" + hash;
}

replica_sApp.prototype.internal_client_write = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_client_write_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new rw_response_header();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_client_write = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_client_write_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new rw_response_header();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.client_write = function(obj) {
    if (!obj.async) {
        return this.internal_client_write(obj.args, obj.hash);
    } else {
        this.internal_async_client_write(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_client_write_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_CLIENT_WRITE", hash);
}

replica_sApp.prototype.internal_client_read = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_client_read_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new rw_response_header();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_client_read = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_client_read_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new rw_response_header();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.client_read = function(obj) {
    if (!obj.async) {
        return this.internal_client_read(obj.args, obj.hash);
    } else {
        this.internal_async_client_read(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_client_read_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_CLIENT_READ", hash);
}

replica_sApp.prototype.internal_prepare = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_prepare_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new prepare_ack();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_prepare = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_prepare_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new prepare_ack();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.prepare = function(obj) {
    if (!obj.async) {
        return this.internal_prepare(obj.args, obj.hash);
    } else {
        this.internal_async_prepare(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_prepare_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_PREPARE", hash);
}

replica_sApp.prototype.internal_config_proposal = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_config_proposal_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new void();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_config_proposal = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_config_proposal_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new void();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.config_proposal = function(obj) {
    if (!obj.async) {
        return this.internal_config_proposal(obj.args, obj.hash);
    } else {
        this.internal_async_config_proposal(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_config_proposal_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_CONFIG_PROPOSAL", hash);
}

replica_sApp.prototype.internal_learn = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_learn_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new learn_response();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_learn = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_learn_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new learn_response();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.learn = function(obj) {
    if (!obj.async) {
        return this.internal_learn(obj.args, obj.hash);
    } else {
        this.internal_async_learn(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_learn_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_LEARN", hash);
}

replica_sApp.prototype.internal_learn_completion_notification = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_learn_completion_notification_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new void();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_learn_completion_notification = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_learn_completion_notification_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new void();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.learn_completion_notification = function(obj) {
    if (!obj.async) {
        return this.internal_learn_completion_notification(obj.args, obj.hash);
    } else {
        this.internal_async_learn_completion_notification(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_learn_completion_notification_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_LEARN_COMPLETION_NOTIFICATION", hash);
}

replica_sApp.prototype.internal_add_learner = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_add_learner_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new void();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_add_learner = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_add_learner_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new void();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.add_learner = function(obj) {
    if (!obj.async) {
        return this.internal_add_learner(obj.args, obj.hash);
    } else {
        this.internal_async_add_learner(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_add_learner_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_ADD_LEARNER", hash);
}

replica_sApp.prototype.internal_remove = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_remove_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new void();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_remove = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_remove_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new void();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.remove = function(obj) {
    if (!obj.async) {
        return this.internal_remove(obj.args, obj.hash);
    } else {
        this.internal_async_remove(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_remove_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_REMOVE", hash);
}

replica_sApp.prototype.internal_group_check = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_group_check_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new group_check_response();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_group_check = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_group_check_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new group_check_response();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.group_check = function(obj) {
    if (!obj.async) {
        return this.internal_group_check(obj.args, obj.hash);
    } else {
        this.internal_async_group_check(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_group_check_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_GROUP_CHECK", hash);
}

replica_sApp.prototype.internal_query_decree = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_query_decree_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new query_replica_decree_response();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_query_decree = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_query_decree_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new query_replica_decree_response();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.query_decree = function(obj) {
    if (!obj.async) {
        return this.internal_query_decree(obj.args, obj.hash);
    } else {
        this.internal_async_query_decree(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_query_decree_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_QUERY_DECREE", hash);
}

replica_sApp.prototype.internal_query_replica_info = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_query_replica_info_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new query_replica_info_response();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

replica_sApp.prototype.internal_async_query_replica_info = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_query_replica_info_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new query_replica_info_response();
            self.unmarshall_generated_type(result, ret);
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

replica_sApp.prototype.query_replica_info = function(obj) {
    if (!obj.async) {
        return this.internal_query_replica_info(obj.args, obj.hash);
    } else {
        this.internal_async_query_replica_info(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

replica_sApp.prototype.get_query_replica_info_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_REPLICA_S_QUERY_REPLICA_INFO", hash);
}

meta_sApp = function(website) {
    this.url = website;
}

meta_sApp.prototype = {};

meta_sApp.prototype.marshall_generated_type = function(value) {
    return marshall_thrift_json_generated_type(value);
}

meta_sApp.prototype.marshall_basic_type = function(value, type) {
    return marshall_thrift_json_basic_type(value, type);
}

meta_sApp.prototype.unmarshall_generated_type = function(buf, ret) {
    unmarshall_thrift_json_generated_type(buf, ret);
}

meta_sApp.prototype.unmarshall_basic_type = function(buf, type) {
    return unmarshall_thrift_json_basic_type(buf, type);
}

meta_sApp.prototype.get_address = function(url, hash) {
    if (typeof hash == "undefined") {
        hash = 0;
    }
    return url + "/" + hash;
}

meta_sApp.prototype.internal_create_app = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_create_app_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_create_app_response();
            self.unmarshall_generated_type(result, ret);
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
        this.get_create_app_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_create_app_response();
            self.unmarshall_generated_type(result, ret);
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

meta_sApp.prototype.get_create_app_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_META_S_CREATE_APP", hash);
}

meta_sApp.prototype.internal_drop_app = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_drop_app_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_drop_app_response();
            self.unmarshall_generated_type(result, ret);
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
        this.get_drop_app_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_drop_app_response();
            self.unmarshall_generated_type(result, ret);
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

meta_sApp.prototype.get_drop_app_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_META_S_DROP_APP", hash);
}

meta_sApp.prototype.internal_query_configuration_by_node = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_query_configuration_by_node_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_query_by_node_response();
            self.unmarshall_generated_type(result, ret);
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
        this.get_query_configuration_by_node_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_query_by_node_response();
            self.unmarshall_generated_type(result, ret);
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

meta_sApp.prototype.get_query_configuration_by_node_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_NODE", hash);
}

meta_sApp.prototype.internal_query_configuration_by_index = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_query_configuration_by_index_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_query_by_index_response();
            self.unmarshall_generated_type(result, ret);
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
        this.get_query_configuration_by_index_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_query_by_index_response();
            self.unmarshall_generated_type(result, ret);
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

meta_sApp.prototype.get_query_configuration_by_index_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_INDEX", hash);
}

meta_sApp.prototype.internal_update_configuration = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_update_configuration_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = new configuration_update_response();
            self.unmarshall_generated_type(result, ret);
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

meta_sApp.prototype.internal_async_update_configuration = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_update_configuration_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = new configuration_update_response();
            self.unmarshall_generated_type(result, ret);
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

meta_sApp.prototype.update_configuration = function(obj) {
    if (!obj.async) {
        return this.internal_update_configuration(obj.args, obj.hash);
    } else {
        this.internal_async_update_configuration(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

meta_sApp.prototype.get_update_configuration_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_REPLICATION_META_S_UPDATE_CONFIGURATION", hash);
}

