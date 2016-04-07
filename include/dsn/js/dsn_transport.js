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

function dsn_call(url, method, send_data, is_async, on_success, on_fail) {
    if ((is_async && (!onSuccess || !onFail)) || url === undefined || url === '') {
        return null;
    }
    if (!method) {
        method = "POST";
    }
    
    $.ajax(
        {
            type: method,
            dataType: "text",
            url: url,
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

function marshall_thrift_json(value)
{
    var transport = new Thrift.DSNTransport();
    var protocol  = new Thrift.TJSONProtocol(transport);
    
    value.write(protocol);
    transport.write(protocol.tstack.pop());
    return transport.getSendBuffer();
}

function unmarshall_thrift_json(buffer, value)
{
    var transport = new Thrift.DSNTransport(buffer);
    var protocol  = new Thrift.TJSONProtocol(transport);
    
    protocol.rstack = [];
    protocol.rpos = [];
    protocol.robj = JSON.parse(protocol.transport.readAll());
    protocol.rstack.push(protocol.robj);
    
    value.read(protocol);
}
