var chart;
var vm = new Vue({
    el: '#app',
    data:{
        taskList: [],
        currentTask: '',
        lastTask: '',
        taskFilter: '',

        sankeyNodes: [],
        sankeyLinks: [],

        if_collapsed: false,

        callee_list: [],
        caller_list: [],
        sharer_list: [],

        drawType: 'Distribution',

        remoteMachine: '',
    },
    components: {
    },
    methods: {
        setcurrentTask: function (task) {
            this.lastTask = this.currentTask;
            this.currentTask = task;
        },
        setdrawType: function (type) {
            this.drawType = type;
        },
        collapse: function () {
            $("#wrapper").toggleClass("toggled");
            this.if_collapsed = !this.if_collapsed;
        },

        updateTaskInfo: function ()
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

                    task_id = task_list.indexOf(self.currentTask);
                    
                    for(index =0 ; index < call_matrix[task_id].length; ++index)
                    {
                        if(call_matrix[task_id][index]!=0 && task_id!=index)
                        {
                            self.sankeyNodes.push({
                                "type": task_list[index],
                                "id": "i"+index,
                                "parent": null,
                                "name": task_list[index]
                            });
                            self.sankeyNodes.push({
                                "type": task_list[index],
                                "id": index,
                                "parent": "i"+index,
                                "number": "101",
                                "name": task_list[index]
                            });

                            self.sankeyLinks.push({
                                "source":task_id,
                                "target":index,
                                "value":call_matrix[task_id][index]
                            });

                            self.callee_list.push({
                                "name": task_list[index],
                                "num": call_matrix[task_id][index]
                            });
                        }
                    }
                    for(index =0 ; index < call_matrix[task_id].length; ++index)
                    {
                        if(call_matrix[index][task_id]!=0 && task_id!=index)
                        {
                            self.sankeyNodes.push({
                                "type": task_list[index],
                                "id": "i"+index,
                                "parent": null,
                                "name": task_list[index]
                            });

                            self.sankeyNodes.push({
                                "type": task_list[index],
                                "id": index,
                                "parent": "i"+index,
                                "number": "101",
                                "name": task_list[index]
                            });

                            self.sankeyLinks.push({
                                "source":index,
                                "target":task_id,
                                "value":call_matrix[index][task_id]
                            });

                            self.caller_list.push({"name": task_list[index], "num": call_matrix[index][task_id]});
                        }
                    }

                    if(self.sankeyLinks.length>0)
                    {
                        self.sankeyNodes.push({
                            "type": task_list[task_id],
                            "id": "i"+task_id,
                            "parent": null,
                            "name": task_list[task_id]
                        });
                        self.sankeyNodes.push({
                            "type": task_list[task_id],
                            "id": task_id,
                            "parent": "i"+task_id,
                            "number": "101",
                            "name": task_list[task_id]
                        });

                        updateSankey(self.sankeyNodes,self.sankeyLinks,vm);
                    }
                    
                }
            );

            $.post("/api/cli", { 
                command: "pq pool_sharer " + self.currentTask
                }, function(data){ 
                    self.sharer_list = JSON.parse(data);
                }
            );

        },

        updateDrawTypeInfo: function ()
        {
            $( "#chart" ).remove();
            $( "#c3_chart" ).append( "<div id='chart'></div>" );
            
            if(this.drawType=='Distribution')
                this.updateDrawTypeAsDistribution();
            else if(this.drawType=='Realtime')
                this.updateDrawTypeAsRealtime();
            else if(this.drawType=='Breakdown')
                this.updateDrawTypeAsBreakdown();
        },
        updateDrawTypeAsDistribution: function ()
        {
            var self = this;
            $.post("/api/cli", { 
                command: "pq counter_sample " + self.currentTask
                }, function(data){ 
                    data = JSON.parse(data);

                    var xs = {};
                    var columns = [];
                    var column = []; 
                    var length_list = [];
                    var x_length = 0;

                    for(index in data)
                    {
                        if(data[index].samples.length>0)
                        {
                            xs[data[index].name] = "x";
                            length_list.push(data[index].samples.length);
                        }
                    }

                    column = ['x'];
                    x_length = Math.max.apply(Math,length_list);
                    for(i=0;i<x_length;++i)
                        column.push(i*100/(x_length-1));
                    columns.push(column);
                        
                    for(i=0;i<data.length;++i)
                    {
                        column = [data[i].name];
                        for(j=0;j<data[i].samples.length;++j)
                            column.push(data[i].samples[j]);
                        columns.push(column);
                    }

                    if(self.remoteMachine != '')
                    {
                        $.ajax({
                          type: 'POST',
                          url: "http://" + self.remoteMachine + "/api/cli",
                          data: {command: "pq counter_sample " + self.currentTask},
                          async:false
                        })
                        .done(function( data ) {
                            data = JSON.parse(data);
                            length_list = [];
                            for(index in data)
                            {
                                if(data[index].samples.length>0)
                                {
                                    xs[data[index].name] = "x2";
                                    length_list.push(data[index].samples.length);
                                }
                            }

                            column = ['x2'];
                            x_length = Math.max.apply(Math,length_list);
                            for(i=0;i<x_length;++i)
                                column.push(i*100/(x_length-1));
                            columns.push(column);

                            for(i=0;i<data.length;++i)
                            {
                                column = [data[i].name];
                                for(j=0;j<data[i].samples.length;++j)
                                    column.push(data[i].samples[j]);
                                columns.push(column);
                            }
                        });
                    }

                    chart = c3.generate({
                        data: {
                            xs: xs,
                            columns: columns,
                            type: 'scatter',
                            colors: {
                                'QUEUE(ns)@server': '#1F77B4',
                                'EXEC(ns)@server': '#FF7F0E',
                                'RPC.SERVER(ns)@server': '#2CA02C',
                                'QUEUE(ns)@client': '#D62728',
                                'EXEC(ns)@client': '#9467BD',
                                'RPC.CLIENT(ns)@client': '#8C564B',
                                'AIO.LATENCY(ns)': '#00FF00',
                            },
                        },
                        subchart: {
                            show: true
                        },
                        axis: {
                            x: {
                                label: 'Samples(%)',
                                tick: {
                                    fit: false
                                }
                            },
                            y: {
                                label: 'Latency (ns)',
                                tick: {
                                    format: d3.format(",")
                                }
                            }
                        }
                    });
                }
            );
        },
        updateDrawTypeAsRealtime: function ()
        {
            function updateData(task, a)
            {
                if(self.drawType != 'Realtime' || task != self.currentTask) return;
                $.post("/api/cli", { 
                    command: "pq counter_realtime " + self.currentTask
                    }, function(data){ 
                        data = JSON.parse(data);

                        var columns = [['x', a]];
                        for (i = 0; i < data.data.length; i++) {
                            columns.push([(data.data)[i].name,(data.data)[i].value]);
                        }
                        chart.flow({
                            columns: columns,
                            duration:1000,
                            done:function(){
                                chart.xgrids.add([{value: a, text:data.time,class:'hoge'}]);
                                setTimeout(function () {
                                    updateData(task, a+1);
                                },0);
                            }
                        });
                    }
                );
            }

            var self = this;

            $.post("/api/cli", { 
                command: "pq counter_realtime " + self.currentTask
                }, function(data){ 
                    data = JSON.parse(data);

                    var columns = [['x', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 ]];
                    for(index in data.data)
                    {
                        columns.push([data.data[index].name,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]);
                    }
                    chart = c3.generate({
                        data: {
                            x: 'x',
                            columns: columns,
                            colors: {
                                'QUEUE(ns)@server': '#1F77B4',
                                'EXEC(ns)@server': '#FF7F0E',
                                'RPC.SERVER(ns)@server': '#2CA02C',
                                'QUEUE(ns)@client': '#D62728',
                                'EXEC(ns)@client': '#9467BD',
                                'RPC.CLIENT(ns)@client': '#8C564B',
                                "AIO.LATENCY(ns)": '#00FF00',
                            },
                        },
                        axis: {
                            x: {
                                show:false,
                            },
                            y : {
                                tick: {
                                    format: d3.format(",")
                                }
                            }
                        },
                        
                    });
                    updateData(self.currentTask, 20);
                }
            );

        },
        updateDrawTypeAsBreakdown: function ()
        {
            var self = this;

            $.post("/api/cli", { 
                command: "pq counter_breakdown " + self.currentTask + " 50"
                }, function(data){ 
                    data = JSON.parse(data);
                    
                    if(self.remoteMachine != '')
                    {
                        $.ajax({
                          type: 'POST',
                          url: "http://" + self.remoteMachine + "/api/cli",
                          data: {command: "pq counter_breakdown " + self.currentTask + " 50"},
                          async:false
                        })
                        .done(function( data2 ) {
                            data2 = JSON.parse(data2);
                        
                            for(index in data)
                            {
                                data[index] = (data[index]!=0)?data[index]:data2[index];
                            }
                        });
                    }

                    chart = c3.generate({
                        size: {
                            height: 500,
                            width: 1000
                        },
                        data: {
                            columns: [
                                ['net(call)',(data[0]-data[3])/2],
                                ['queue(server)',data[1]],
                                ['exec(server)',data[2]],
                                ['net(reply)',(data[0]-data[3])/2],
                                ['queue(client)',data[4]],
                                ['exec(client)',data[5]],
                                ['aio',data[6]],
                            ],
                            type: 'bar',
                            colors: {
                                "net(call)": '#F08080',
                                "queue(server)": '#1F77B4',
                                "exec(server)": '#FF7F0E',
                                "net(reply)": '#AFCAE0',
                                "queue(client)": '#D62728',
                                "exec(client)": '#9467BD',
                                "aio": '#00FF00',
                            },
                            groups: [
                                ['net(call)', 'queue(server)','exec(server)','net(reply)','queue(client)','exec(client)']
                            ]
                        },
                        grid: {
                            y: {
                                lines: [{value:0}]
                            }
                        },
                          
                        axis: {
                            x: {
                                type: 'category',
                                label: 'task code',
                                categories: [self.currentTask]
                            },
                            y:{
                                label: 'ns',
                                tick: {
                                    format: d3.format(",")
                                },
                            }
                        }
                    });
                }
            );
        },
        linkRemoteMachine: function()
        {
            this.updateDrawTypeInfo();
        }
    },
    watch: {
        'currentTask': function (newTask, oldTask)
        {
            this.updateTaskInfo();
            this.updateDrawTypeInfo();
        },
        'drawType': function (newType, oldType)
        {
            this.updateDrawTypeInfo();
        },
    },
    ready: function ()
    {
        var self = this;
        
        $.post("/api/cli", { 
            command: "pq task_list"
            }, function(data){
                if(data.indexOf('unknown command') > -1) 
                    self.setcurrentTask('Profiler is not opened. Please set "toolets = profiler" in the config file');
                    
                self.taskList = JSON.parse(data);
                self.setcurrentTask(self.taskList[0]);
                
            }
        )
    }
});

