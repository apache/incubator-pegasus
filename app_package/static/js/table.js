var vm = new Vue({
    el: '#app',
    data:{
        tableData: '',
        filterKey: '',
    
        percentList: ['50%','90%','95%','99%','999%']
    },
    components: {
    },
    methods: {

    },
    watch: {
        filterKey: function (newKey, oldKey)
        {
            $('#table').DataTable().search(newKey).draw();
        }
    },
    ready: function ()
    {
        var self = this;
        var client = new cliApp("http://"+localStorage['dsn_rpc_address']);
        result = client.call({
                args: new command({
                cmd: "pq",
                arguments: ['table']
            }),
            async: true,
            on_success: function (data){
                self.tableData = JSON.parse(data);
                $('#table').DataTable({
                    data: self.tableData,
                });
            },
            on_fail: function (xhr, textStatus, errorThrown) {}
        });
    }
});

