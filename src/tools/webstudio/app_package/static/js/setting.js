var vm = new Vue({
    el: '#app',
    data:{
        setting_list: [
            {'name':'dsn_rpc_address','label':'DSN RPC Address','description':'','value':''},
            {'name':'meta_server_address','label':'Meta Server Address','description':'','value':''}
        ] 
    },
    watch: {
        'setting_list': {
            handler: function (lst, oldLst) {
                for(item in lst)
                {
                    localStorage.setItem(lst[item].name, lst[item].value);
                }
            },
            deep: true
        },
    },
    components: {
    },
    methods: {
        
    },
    ready: function ()
    {
        for(item in this.setting_list)
        {
            this.setting_list[item].value = localStorage[this.setting_list[item].name];
        }
    }
});

