cliApp = function(website) {
    this.url = website;
}

cliApp.prototype = {};

cliApp.prototype.marshall_generated_type = function(value) {
    return marshall_thrift_json_generated_type(value);
}

cliApp.prototype.marshall_basic_type = function(value, type) {
    return marshall_thrift_json_basic_type(value, type);
}

cliApp.prototype.unmarshall_generated_type = function(buf, ret) {
    unmarshall_thrift_json_generated_type(buf, ret);
}

cliApp.prototype.unmarshall_basic_type = function(buf, type) {
    return unmarshall_thrift_json_basic_type(buf, type);
}

cliApp.prototype.get_address = function(url, hash) {
    if (typeof hash == "undefined") {
        hash = 0;
    }
    return url + "/" + hash;
}

cliApp.prototype.internal_call = function(args,  hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_call_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        false,
        function(result) {
            ret = self.unmarshall_basic_type(result, "string");
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
        }
    );
    return ret;
}

cliApp.prototype.internal_async_call = function(args, on_success, on_fail, hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_call_address(hash),
        "POST",
        this.marshall_generated_type(args),
        "DSF_THRIFT_JSON",
        true,
        function(result) {
            ret = self.unmarshall_basic_type(result, "string");
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

cliApp.prototype.call = function(obj) {
    if (!obj.async) {
        return this.internal_call(obj.args, obj.hash);
    } else {
        this.internal_async_call(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

cliApp.prototype.get_call_address = function(hash) {
    return this.get_address(this.url + "/" + "RPC_CLI_CLI_CALL", hash);
}

