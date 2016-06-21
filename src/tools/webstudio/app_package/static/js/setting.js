var vm = new Vue({
    el: '#app',
    data:{
        setting_list: [
            {'name':'target_server','label':'Server','description':'address of target machine which we care about its runtime information','value':'srgx-02:34802'},
            {'name':'target_meta_server','label':'Meta Server','description':'address of meta server','value':'srgx-02:34602'},
            {'name':'target_app_store','label':'App Store Server','description':'address of app store','value':'srgx-02:34602'},
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
                setTimeout(function(){ self.state = ''; }, 1000);
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

