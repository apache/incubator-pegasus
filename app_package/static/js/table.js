var vm = new Vue({
    el: '#app',
    data:{
        tableData: '',
    },
    components: {
    },
    methods: {

    },
    ready: function ()
    {
        var self = this;

        $.post("/api/cli", { 
            command: "pq table"
            }, function(data){ 
                self.tableData = JSON.parse(data);
                $('#table').DataTable({
                    data: self.tableData,
                });
            }
        );
    }
});

