var chart;
var vm = new Vue({
    el: '#app',
    data:{
    },
    components: {
    },
    methods: {
        update: function ()
        {
            var self = this;
            $.post("/api/cli", {
                command: "system.queue"
            }, function(data){
                data = JSON.parse(data);
                var queueList = [];
                for(appIndex in data)
                {
                    var app = data[appIndex];
                    for(poolIndex in app.thread_pool)
                    {
                        var pool = app.thread_pool[poolIndex];
                        for(queueIndex in pool.pool_queue)    
                        {
                            var queue = pool.pool_queue[queueIndex];
                            queueList.push({name:app.app_name+"*"+pool.pool_name+"*"+queue.name,length:queue.num});
                        }
                    }
                }
                queueList.sort(function(a, b) {
                    return b.length- a.length;
                });
                queueList = queueList.slice(0, 10);

                var barList = [];
                barList.push([]);
                barList.push([]);
                for(index in queueList)
                {
                    barList[0].push(queueList[index].name);
                    barList[1].push(queueList[index].length);
                }
                chart.load({
                    rows:barList, 
                });
            })
            .fail(function() {

            });
        },
    },
    ready: function ()
    {
        var self = this;
        chart = c3.generate({
            size: {
                height: 720,
            },
            data: {
                columns: [],
                type: 'bar',
            },
            grid: {
                y: {
                    lines: [{value:0}]
                }
            },
              
            axis: {
                x: {
                    label: 'queue',
                },
                y:{
                    label: 'length',
                    tick: {
                            format: d3.format(",")
                    }
                }
            }
        });
        setInterval(function () {
            self.update();
        }, 1000);
    }
});

