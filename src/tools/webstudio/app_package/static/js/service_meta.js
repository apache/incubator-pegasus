//Vue.config.debug = true;

//parameter parsing function
function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

var vm = new Vue({
    el: '#app',
    data:{
        appInfo: null,
        appDetails: [],
        appTotal:0,
        updateTimer: 0,
        filterKey: '',
    },
    components: {
    },
    methods: {
        update: function()
        {
            var self = this;
            var client = new meta_sApp("http://"+localStorage['target_meta_server']);
            result = client.list_apps({
                args: new configuration_list_apps_request(),
                async: true,
                on_success: function (appdata){
                    try {
                        appdata = new configuration_list_apps_response(appdata);
                        self.$set('appInfo', appdata)                    
                    }
                    catch(err) {
                        console.log(err);
                    }

                    if(self.appTotal != self.appInfo.infos.length)
                    {
                        self.appTotal = self.appInfo.infos.length;
                        self.appDetails = [];
                    }

                    var app;
                    for (app = 0; app < self.appInfo.infos.length; ++app)
                    {
                        (function(app){
                            result = client.query_configuration_by_index({
                                args: new configuration_query_by_index_request({
                                    'app_name': self.appInfo.infos[app].app_name,
                                    'partition_indices': []
                                }),
                                async: true,
                                on_success: function (servicedata){
                                    var is_stateful = false;
                                    try {
                                        servicedata = new configuration_query_by_index_response(servicedata);
                                        console.log(JSON.stringify(servicedata));
                                        self.appDetails.$set(app, servicedata);
                                        is_stateful = servicedata.is_stateful;
                                    }
                                    catch(err) {
                                        return;
                                    }
                                    
                                    var partition;
                                    for (partition = 0; partition < self.appDetails[app].partitions.length; ++partition)
                                    {
                                        var par = self.appDetails[app].partitions[partition];
                                        par.membership = '';

                                        if(is_stateful)
                                        {
                                            if(par.primary != 'invalid address')
                                            {
                                                par.membership += 'P: ("' + par.primary.host + ':'+ par.primary.port  + '"), ';
                                                
                                            }
                                            else
                                            {
                                                par.membership += 'P: (), ';
                                            }

                                            par.membership += 'S: [';
                                            for (secondary in par.secondaries)
                                            {
                                                par.membership += '"' + par.secondaries[secondary].host + ':' + par.secondaries[secondary].port + '",'
                                            }
                                            par.membership += '],';

                                            par.membership += 'D: [';
                                            for (drop in par.last_drops)
                                            {
                                                par.membership += '"' + par.last_drops[drop].host +':'+ par.last_drops[drop].port + '",'
                                            }
                                            par.membership += ']';
                                        }
                                        else
                                        {
                                            par.membership += 'replicas: [';
                                            for (secondary in par.secondaries)
                                            {
                                                par.membership += '"' + par.secondaries[secondary].host + ':' + par.secondaries[secondary].port + '",'
                                            }
                                            par.membership += ']';
                                        }
                                    }
                                },
                                on_fail: function (xhr, textStatus, errorThrown) {}
                            });
                        })(app);
                    }
                },
                on_fail: function (xhr, textStatus, errorThrown) {}
            });
        },
        del: function (app_name)
        {
            var self = this;

            var client = new meta_sApp("http://"+localStorage['target_meta_server']);
            result = client.drop_app({
                args: new configuration_drop_app_request({
                    'app_name': app_name,
                    'options': new drop_app_options ({'success_if_not_exist':false})
                }),
                async: true,
                on_success: function (data){
                    data = new configuration_drop_app_response(data);
                    alert(JSON.stringify(data));
                },
                on_fail: function (xhr, textStatus, errorThrown) {}
            });
        }
    },
    ready: function ()
    {
        var self = this;

        self.filterKey = getParameterByName("filterKey");
        self.update(); 
        //query each machine their service state
        self.updateTimer = setInterval(function () {
            self.update(); 
        }, 3000);
    }
});

