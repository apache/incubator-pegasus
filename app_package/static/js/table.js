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

