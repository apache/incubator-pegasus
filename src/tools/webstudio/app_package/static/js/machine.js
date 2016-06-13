var vm = new Vue({
    el: '#app',
    data:{
        nodeList: [],
        nodeTotal: 0,
        partitionList: [],
        updateTimer: 0,
    },
    components: {
    },
    methods: {
        update: function()
        {
            var self = this;
            var client = new meta_sApp("http://"+localStorage['meta_server_address']);
            result = client.list_nodes({
                args: new configuration_list_nodes_request({
                    'node_status': 'NS_INVALID'
                }),
                async: true,
                on_success: function (nodedata){
                    try {
                        nodedata = new configuration_list_nodes_response(nodedata);
                        console.log(JSON.stringify(nodedata));
                        self.$set('nodeList', nodedata);
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
                            result = client.query_configuration_by_node({
                                args: new configuration_query_by_node_request({
                                    'node': new rpc_address({host:self.nodeList.infos[nodeIndex].address.host,port:self.nodeList.infos[nodeIndex].address.port})
                                }),
                                async: true,
                                on_success: function (servicedata){
                                    try {
                                        servicedata = new configuration_query_by_node_response(servicedata);
                                        console.log(JSON.stringify(servicedata));
                                        self.partitionList.$set(nodeIndex, servicedata);
                                    }
                                    catch(err) {
                                        return;
                                    }
                                    
                                    for (partition in self.partitionList[nodeIndex].partitions)
                                    {
                                        var par = self.partitionList[nodeIndex].partitions[partition];
                                        par.role = '';
                                        par.working_point = '';

                                        var addressList = {};
                                        addressList['primary'] = par.config.primary.host+':'+par.config.primary.port;
                                        addressList['secondaries'] = [];
                                        for (secondary in par.config.secondaries)
                                        {
                                            addressList['secondaries'][secondary] = par.config.secondaries[secondary].host +':'+ par.config.secondaries[secondary].port;
                                        } 
                                        addressList['last_drops'] = [];
                                        for (drop in par.config.last_drops)
                                        {
                                            addressList['last_drops'][drop] = par.config.last_drops[drop].host +':'+ par.config.last_drops[drop].port;
                                        } 


                                        if(par.info.is_stateful==true)
                                        {
                                            //stateful service
                                            if (addressList.primary== self.nodeList.infos[nodeIndex].address.host+':'+self.nodeList.infos[nodeIndex].address.port) 
                                            {
                                                par['role'] = 'primary';
                                            }
                                            else if (addressList.secondaries.indexOf(
                                                self.nodeList.infos[nodeIndex].address.host+':'+self.nodeList.infos[nodeIndex].address.port) > -1)
                                            {
                                                par['role'] = 'secondary';
                                            }
                                            else if (addressList.last_drops.indexOf(
                                                self.nodeList.infos[nodeIndex].address.host+':'+self.nodeList.infos[nodeIndex].address.port) > -1)
                                            {
                                                par['role'] = 'drop';
                                            }
                                            else
                                                par['role'] = 'undefined';
                                        }
                                        else
                                        {
                                            par['working_point'] = par.last_drops[addressList.secondaries.indexOf(
                                                    self.nodeList.infos[nodeIndex].address.host +':'+
                                                        self.nodeList.infos[nodeIndex].address.port)];
                                        }
                                    }
                                },
                                on_fail: function (xhr, textStatus, errorThrown) {}
                            });
                        })(node);
                    }
                },
                on_fail: function (xhr, textStatus, errorThrown) {}
            });

        },
        del: function (address, role, gpid)
        {
            /*
            var self = this;
            console.log(((role!='')?'replica.':'daemon.') + "kill_partition " + gpid.app_id + " " + gpid.pidx);
            var client = new cliApp("http://"+localStorage['meta_server_address']);
            result = client.call({
                    args: new command({
                    cmd: ((role!='')?'replica.':'daemon.') + "kill_partition",
                    arguments: [gpid.app_id,gpid.pidx]
                }),
                async: true,
                on_success: function (data){
                    console.log(data);
                },
                on_fail: function (xhr, textStatus, errorThrown) {}
            });
            */
            alert('This function not available now');
        }
    },
    ready: function ()
    {
        var self = this;
        self.update(); 
        //query each machine their service state
        self.updateTimer = setInterval(function () {
            self.update(); 
        }, 1000);
    }
});

