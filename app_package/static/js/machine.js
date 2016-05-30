var vm = new Vue({
    el: '#app',
    data:{
        nodeList: [],
        nodeTotal: 0,
        partitionList: [],
        updateTimer: 0,
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
                command: 'meta.list_nodes'
            }, function(nodedata){
                try {
                    //self.nodeList = JSON.parse(nodedata);
                    self.$set('nodeList', JSON.parse(nodedata));
                }
                catch(err) {
                }
                
                if(self.nodeTotal !=self.nodeList.infos.length)
                {
                    self.nodeTotal = self.nodeList.infos.length;
                    self.partitionList = [];
                }

                for (node in self.nodeList.infos)
                {
                    (function(nodeIndex){
                        $.post("http://" + self.metaServer + ":" + self.commonPort + "/api/cli", {
                            command: 'meta.query_config_by_node {"req":{"node":"' + self.nodeList.infos[nodeIndex].address +'"}}'
                        }, function(servicedata){
                            try {
                                self.partitionList.$set(nodeIndex, JSON.parse(servicedata));
                            }
                            catch(err) {
                                return;
                            }
                            
                            for (partition in self.partitionList[nodeIndex].partitions)
                            {
                                var par = self.partitionList[nodeIndex].partitions[partition];
                                par.role = '';
                                par.working_point = '';

                                if(par.package_id=='')
                                {
                                    //stateful service
                                    if (par.primary == self.nodeList.infos[nodeIndex].address)
                                    {
                                        par['role'] = 'primary';
                                    }
                                    else if (par.secondaries.indexOf(self.nodeList.infos[nodeIndex].address) > -1)
                                    {
                                        par['role'] = 'secondary';
                                    }
                                    else if (par.last_drops.indexOf(self.nodeList.infos[nodeIndex].address) > -1)
                                    {
                                        par['role'] = 'drop';
                                    }
                                    else
                                        par['role'] = 'undefined';
                                }
                                else
                                {
                                    par['working_point'] = par.last_drops[par.secondaries.indexOf(self.nodeList.infos[nodeIndex].address)];
                                }
                            }

                        })
                    })(node);
                }
            })
        },
        del: function (address, role, gpid)
        {
            var self = this;
            console.log(((role!='')?'replica.':'daemon.') + "kill_partition " + gpid.app_id + " " + gpid.pidx);
            $.post("http://" + address.split(":")[0] + ":" + self.commonPort + "/api/cli", {
                command: ((role!='')?'replica.':'daemon.') + "kill_partition " + gpid.app_id + " " + gpid.pidx
            }, function(data){
                console.log(data);
            });
        }
    },
    ready: function ()
    {
        var self = this;
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

