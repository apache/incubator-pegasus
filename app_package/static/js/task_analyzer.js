var vm = new Vue({
    el: '#app',
    data:{
        taskList: [],
        currentTask: '',
        taskFilter: '',
        sankeyNodes: [],
        sankeyLinks: [],

        if_collapsed: false,

        callee_list: [],
        caller_list: [],
        sharer_list: [],
    },
    components: {
    },
    methods: {
        setcurrentTask: function (task) {
            this.currentTask = task;
        },
        collapse: function () {
            $("#wrapper").toggleClass("toggled");
            this.if_collapsed = !this.if_collapsed;
        },
    },
    watch: {
        'currentTask': function (newTask, oldTask)
        {
            var self = this;
            var task_list, call_matrix;

            $( "#chart_dep" ).remove();
            $( "#linkgraph" ).append( "<div id='chart_dep'></div>" );

            $.post("/api/cli", { 
                command: "pq call"
                }, function(data){ 
                    data = JSON.parse(data);
                    task_list = data.task_list;
                    call_matrix = data.call_matrix;
                    self.sankeyNodes = [];
                    self.sankeyLinks = [];   
                    self.callee_list = [];
                    self.caller_list = [];   

                    task_id = task_list.indexOf(newTask);
                    

                    for(index =0 ; index < call_matrix[task_id].length; ++index)
                    {
                        if(call_matrix[task_id][index]!=0 && task_id!=index)
                        {
                            self.sankeyNodes.push(
                                {
                                    "type": task_list[index],
                                    "id": "i"+index,
                                    "parent": null,
                                    "name": task_list[index]
                                }
                            );
                            self.sankeyNodes.push(
                                {
                                    "type": task_list[index],
                                    "id": index,
                                    "parent": "i"+index,
                                    "number": "101",
                                    "name": task_list[index]
                                }
                            );

                            self.sankeyLinks.push(
                                {
                                    "source":task_id,
                                    "target":index,
                                    "value":call_matrix[task_id][index]
                                }
                            );

                            self.callee_list.push({"name": task_list[index], "num": call_matrix[task_id][index]});
                        }
                    }
                    for(index =0 ; index < call_matrix[task_id].length; ++index)
                    {
                        if(call_matrix[index][task_id]!=0 && task_id!=index)
                        {
                            self.sankeyNodes.push(
                                {
                                    "type": task_list[index],
                                    "id": "i"+index,
                                    "parent": null,
                                    "name": task_list[index]
                                }
                            );
                            self.sankeyNodes.push(
                                {
                                    "type": task_list[index],
                                    "id": index,
                                    "parent": "i"+index,
                                    "number": "101",
                                    "name": task_list[index]
                                }
                            );

                            self.sankeyLinks.push(
                                {
                                    "source":index,
                                    "target":task_id,
                                    "value":call_matrix[index][task_id]
                                }
                            );

                            self.caller_list.push({"name": task_list[index], "num": call_matrix[index][task_id]});
                        }
                    }

                    if(self.sankeyLinks.length>0)
                    {
                        self.sankeyNodes.push(
                            {
                                "type": task_list[task_id],
                                "id": "i"+task_id,
                                "parent": null,
                                "name": task_list[task_id]
                            }
                        );
                        self.sankeyNodes.push(
                            {
                                "type": task_list[task_id],
                                "id": task_id,
                                "parent": "i"+task_id,
                                "number": "101",
                                "name": task_list[task_id]
                            }
                        );

                        updateSankey(self.sankeyNodes,self.sankeyLinks,vm);
                    }
                    
                }
            );
            $.post("/api/cli", { 
                command: "pq pool_sharer " + newTask
                }, function(data){ 
                    self.sharer_list = JSON.parse(data);
                }
            );
        }
    },
    ready: function ()
    {
        var self = this;
        
        $.post("/api/cli", { 
            command: "pq task_list"
            }, function(data){ 
                self.taskList = JSON.parse(data);
                
            }
        );
    }
});

