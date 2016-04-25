Thrift.DSNTransport = function(buffer) {
    this.wpos = 0;
    this.rpos = 0;
    this.useCORS = null;
    if (buffer == undefined) {
        this.send_buf = "";
        this.recv_buf = "";
    } else {
        this.send_buf = this.recv_buf = buffer;
    }
};

Thrift.DSNTransport.prototype = {
    /**
     * Returns true if the transport is open, XHR always returns true.
     * @readonly
     * @returns {boolean} Always True.
     */    
    isOpen: function() {
        return true;
    },

    /**
     * Opens the transport connection, with XHR this is a nop.
     */    
    open: function() {},

    /**
     * Closes the transport connection, with XHR this is a nop.
     */    
    close: function() {},

    /**
     * Returns the specified number of characters from the response
     * buffer.
     * @param {number} len - The number of characters to return.
     * @returns {string} Characters sent by the server.
     */
    read: function(len) {
        var avail = this.wpos - this.rpos;

        if (avail === 0) {
            return '';
        }

        var give = len;

        if (avail < len) {
            give = avail;
        }

        var ret = this.read_buf.substr(this.rpos, give);
        this.rpos += give;

        //clear buf when complete?
        return ret;
    },

    /**
     * Returns the entire response buffer.
     * @returns {string} Characters sent by the server.
     */
    readAll: function() {
        return this.recv_buf;
    },

    /**
     * Sets the send buffer to buf.
     * @param {string} buf - The buffer to send.
     */    
    write: function(buf) {
        this.send_buf = buf;
    },

    /**
     * Returns the send buffer.
     * @readonly
     * @returns {string} The send buffer.
     */ 
    getSendBuffer: function() {
        return this.send_buf;
    }

};

var DSN = {
    payload_format : {
        'DSF_THRIFT_BINARY' : 0,
        'DSF_THRIFT_COMPACT' : 1,
        'DSF_THRIFT_JSON' : 2,
        'DSF_PROTOC_BINARY' : 3,
        'DSF_PROTOC_JSON' : 4,
    },
    
    thrift_basic_type : {
        "bool" : Thrift.Type.BOOL,
        "byte" : Thrift.Type.BYTE,
        "i16" : Thrift.Type.I16,
        "i32" : Thrift.Type.I32,
        "i64" : Thrift.Type.I64,
        "double" : Thrift.Type.DOUBLE,
        "string" : Thrift.Type.STRING 
    }
};

function dsn_call(url, method, send_data, payload_format, is_async, on_success, on_fail) {
    if ((is_async && (!on_success || !on_fail)) || url === undefined || url === '') {
        return null;
    }
    if (!method) {
        method = "POST";
    }
    payload_format_id = DSN.payload_format[payload_format];
    
    $.ajax(
        {
            type: method,
            dataType: "text",
            url: url + "?payload_format=" + payload_format_id,
            data: send_data,
            async: is_async,
            success: function(response) {
                on_success(response);
            },
            error: function(xhr, textStatus, errorThrown){
                on_fail(xhr, textStatus, errorThrown);
            }
        }
    )
}

function marshall_thrift_json_generated_type(value)
{
    var transport = new Thrift.DSNTransport();
    var protocol  = new Thrift.TJSONProtocol(transport);
    
    value.write(protocol);
    transport.write(protocol.tstack.pop());
    return transport.getSendBuffer();
}

function unmarshall_thrift_json_generated_type(buffer, value)
{
    var transport = new Thrift.DSNTransport(buffer);
    var protocol  = new Thrift.TJSONProtocol(transport);
    
    protocol.rstack = [];
    protocol.rpos = [];
    protocol.robj = JSON.parse(protocol.transport.readAll());
    protocol.rstack.push(protocol.robj);
    
    value.read(protocol);
}

function marshall_thrift_json_basic_type(value, type)
{
    var transport = new Thrift.DSNTransport();
    var protocol  = new Thrift.TJSONProtocol(transport);
    protocol.writeStructBegin('basic');
    if (value !== null && value !== undefined)
    {
        protocol.writeFieldBegin('value', DSN.thrift_basic_type[type], 1);
        switch(type)
        {
            case "bool" :
                protocol.writeBool(value);
                break;
            case "byte" :
                protocol.writeByte(value);
                break;
            case "i16" :
                protocol.writeI16(value);
                break;
            case "i32" :
                protocol.writeI32(value);
                break;
            case "i64" :
                protocol.writeI64(value);
                break;
            case "double" :
                protocol.writeDouble(value);
                break;
            case "string" :
                protocol.writeString(value);
                break;
        }
        protocol.writeFieldEnd();
    }
    protocol.writeFieldStop();
    protocol.writeStructEnd();
    transport.write(protocol.tstack.pop());
    return transport.getSendBuffer();
}

function unmarshall_thrift_json_basic_type(buffer, type)
{
    var transport = new Thrift.DSNTransport(buffer);
    var protocol  = new Thrift.TJSONProtocol(transport);
    
    protocol.rstack = [];
    protocol.rpos = [];
    protocol.robj = JSON.parse(protocol.transport.readAll());
    protocol.rstack.push(protocol.robj);
    
    protocol.readStructBegin();
    var value = null;
    while (true)
    {
        var ret = protocol.readFieldBegin();
        var fname = ret.fname;
        var ftype = ret.ftype;
        var fid = ret.fid;
        if (ftype == Thrift.Type.STOP) {
            break;
        }
        switch (fid)
        {
            case 1:
            if (ftype == DSN.thrift_basic_type[type])
            {
                switch(type)
                {
                    case "bool" :
                        value = protocol.readBool().value;
                        break;
                    case "byte" :
                        value = protocol.readByte().value;
                        break;
                    case "i16" :
                        value = protocol.readI16().value;
                        break;
                    case "i32" :
                        value = protocol.readI32().value;
                        break;
                    case "i64" :
                        value = protocol.readI64().value;
                        break;
                    case "double" :
                        value = protocol.readDouble().value;
                        break;
                    case "string" :
                        value = protocol.readString().value;
                        break;
                }
            } else {
                protocol.skip(ftype);
            }
            break;
            case 0:
                protocol.skip(ftype);
                break;
            default:
                protocol.skip(ftype);
        }
        protocol.readFieldEnd();
    }
    protocol.readStructEnd()
    return value;
}