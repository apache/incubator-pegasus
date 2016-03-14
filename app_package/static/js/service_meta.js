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
        appList: [],
        partitionList: [],
        appTotal:0,
        updateTimer: 0,
        filterKey: '',
        metaServer: '',
        commonPort: ''
    },
    components: {
    },
    methods: {
        update: function()
        {
            var self = this;
            $.post("http://" + self.metaServer + ":" + self.commonPort + "/api/cli", {
                command: 'meta.list_apps'
            }, function(appdata){
                try {
                    self.$set('appList', JSON.parse(appdata))
                }
                catch(err) {
                }

                if(self.appTotal !=self.appList.infos.length)
                {
                    self.appTotal = self.appList.infos.length;
                    self.partitionList = [];
                }

                for (app in self.appList.infos)
                {
                    (function(app){
                        $.post("http://" + self.metaServer + ":" + self.commonPort + "/api/cli", {
                            command: 'meta.query_config_by_app {"req":{"app_name":"' + self.appList.infos[app].app_name + '","partition_indices":[]}}'
                        }, function(servicedata){
                            try {
                                self.partitionList.$set(app, JSON.parse(servicedata))
                            }
                            catch(err) {
                                return;
                            }
                            
                            for (partition in self.partitionList[app].partitions)
                            {
                                var par = self.partitionList[app].partitions[partition];
                                par.membership = '';

                                if(par.packageid=='')
                                {
                                    if(par.primary!='invalid address')
                                    {
                                        par.membership += 'P: ("' + par.primary + '"),\n ';
                                        
                                    }
                                    else
                                    {
                                        par.membership += 'P: (), ';
                                    }

                                    par.membership += 'S: [';
                                    for (secondary in par.secondaries)
                                    {
                                        par.membership += '"' + par.secondaries[secondary]+ '",'
                                    }
                                    par.membership += '],';

                                    par.membership += 'D: [';
                                    for (drop in par.last_drops)
                                    {
                                        par.membership += '"' + par.last_drops[drop]+ '",'
                                    }
                                    par.membership += ']';
                                }
                                else
                                {
                                    par.membership += 'replicas: [';
                                    for (secondary in par.secondaries)
                                    {
                                        par.membership += '"' + par.secondaries[secondary]+ '",'
                                    }
                                    par.membership += ']';
                                }
                            }

                        })
                    })(app);
                }
            })
        },
        del: function (app_name)
        {
            var self = this;
                
            var command = "meta.drop_app ";
            var jsObj = JSON.stringify({
                req: {
                    app_name: app_name,
                    options: {
                        success_if_not_exist: false
                    }
                }
            });
            command += jsObj;
            $.post("http://" + self.metaServer + ":" + self.commonPort +"/api/cli", { 
                command: command
                }, function(data){ 
                    console.log(data);
                }
            );

        }
    },
    ready: function ()
    {
        var self = this;

        self.filterKey = getParameterByName("filterKey");
        $.post("/api/metaserverquery", { 
            }, function(data){ 
                console.log(data);
                self.metaServer = data.split(":")[0];
                self.commonPort = window.location.href.split("/")[2].split(":")[1];
                self.update(); 
                //query each machine their service state
                self.updateTimer = setInterval(function () {
                   self.update(); 
                }, 1000);
            }
        );
    }
});

