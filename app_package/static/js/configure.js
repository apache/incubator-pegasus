var vm = new Vue({
    el: '#app',
    data:{
        state:'loading...'
    },
    components: {
    },
    methods: {
       
    },
    ready: function ()
    {
        var self = this;
        var client = new cliApp("http://"+localStorage['dsn_rpc_address']);
        result = client.call({
                args: new command({
                cmd: "config-dump",
                arguments: []
            }),
            async: true,
            on_success: function (data){
                var myCodeMirror = CodeMirror(document.body,{
                    value: data,
                    mode:  "toml",
                    theme: "solarized",
                    keyMap : "vim",
                    lineNumbers: true,
                    matchBrackets: true,
                    showCursorWhenSelecting: true,
                    viewportMargin: Infinity
                });
                self.state = ''
            },
            on_fail: function (xhr, textStatus, errorThrown) {}
        });
    }
});

