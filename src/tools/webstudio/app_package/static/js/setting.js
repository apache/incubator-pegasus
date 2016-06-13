var vm = new Vue({
    el: '#app',
    data:{
        setting_list: [
            {'name':'dsn_rpc_address','label':'DSN RPC Address','description':'the machine address you use when do single-node profiling','value':''},
            {'name':'meta_server_address','label':'Meta Server Address','description':'the meta server address for app store','value':''}
        ],
        state: ''
    },
    watch: {
        'setting_list': {
            handler: function (lst, oldLst) {
                var self = this;
                self.state = 'saving';
                for(item in lst)
                {
                    localStorage.setItem(lst[item].name, lst[item].value);
                }
                setTimeout(function(){ self.state = ''; }, 700);
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

