//parameter parsing function
function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

//time operation
function strtoms(str){
        var arr = str.split(":");
        var arr2 = arr[2].split(".");
        return ((Number(arr[0]) * 60 + Number(arr[1])) * 60 + Number(arr2[0])) * 1000 + Number(arr2[1]);
}

function mstostr(milli){
        var hr= ("00" + Math.floor((milli / (60 * 60 * 1000))%24).toString()).substr(-2,2);
        var min= ("00" + Math.floor((milli / (60 * 1000))%60).toString()).substr(-2,2);
        var sc= ("00" + Math.floor((milli / 1000)%60).toString()).substr(-2,2);
        var ms=  ("00" + (milli %1000).toString()).substr(-3,3);
        return hr + ":" + min + ":" + sc + "." + ms; 
}

function calcdiffms(time1,time2){
    var res = strtoms(time2) - strtoms(time1);
    if (res<0)
        return (res + 24*60*60*1000);
    return res;
}

function addmstostr(str,ms){
    return mstostr(strtoms(str)+ms);
}

function nstostr(ns){
    return mstostr(Math.floor(ns/1000000));
}

//c3js chart
var chart;

var viewChart = Vue.extend({
  template: '',
  props: ['counterlist','graphtype','interval','currentValue','currentTime','stopFlag','info','updateSlotFunc','colorlist'],
  data: function () {
      return {
          NextSlot: [['x', 1]],
          counterInfo: []
      }
  },
  methods: {
      bar_init: function () {
        chart = c3.generate({
            data: {
                columns: [],
                type: 'bar',
            },
            grid: {
                y: {
                    lines: [{value:0}]
                }
            },
            bar: {
                width: 100 
            },
            axis: {
                x: {
                    label: 'counter',
                },
                y:{
                    tick: {
                        format: d3.format(",")
                    }
                }
            }
        });
    
        var self = this;
        this.updateSlotFunc = [];
        this.currentValue = [];
        this.currentTime = [];

        for (counter in this.counterlist)
        {
            this.currentValue.push(0);
            this.currentTime.push(0);
            this.colorlist.push('black');
            this.counterInfo.push({name:this.counterlist[counter].name,index:0});
            var newSlotUpdateFunc = setInterval(function (count) {
                self.update_slot(count);
                }, self.interval*1000, counter);
            this.updateSlotFunc.push(newSlotUpdateFunc);
        }
        this.bar_update_graph();
      },
      bar_update_graph: function ()
      {
        for (counter in this.counterlist)
        {
            try{
                var label = this.counterlist[counter]['label'];
                chart.load({
                    rows: [
                    [label],
                    [this.currentValue[counter]]
                    ],
                });
            }
            catch(err){}
        }
        var self = this;
        setTimeout(function () {
            if (!self.stopFlag)
                self.bar_update_graph();
        }, self.interval * 1000);

      },
      realtime_init: function () {
        var BaseArray = Array.apply(null, Array(20)).map(function (_, i) {return (i+1).toString();});
        BaseArray.unshift('x');
        BaseArray = [BaseArray];

        this.NextSlot = [['x', 1]];
        this.currentValue = [];
        this.currentTime = [];

        for (counter in this.counterlist)
        {
            var label = this.counterlist[counter]['label'];
            var BaseArrayAdd = Array.apply(null, Array(20)).map(function (_, i) {return 0;});
            BaseArrayAdd.unshift(label);
            BaseArray.push(BaseArrayAdd);
            this.NextSlot.push([label, 0]);
            this.currentValue.push(0);
            this.currentTime.push(0);
            this.colorlist.push('black');
            this.counterInfo.push({name:this.counterlist[counter].name,index:0});
        }

        chart = c3.generate({
            data: {
                x: 'x',
                columns: BaseArray
            },
            axis: {
                x: {
                    show:false
                }
            }
        });

        var self = this;
        this.updateSlotFunc = [];
        for (counter in this.counterlist)
        {
            var newSlotUpdateFunc = setInterval(function (count) {
                self.update_slot(count);
                }, self.interval*1000, counter);
            this.updateSlotFunc.push(newSlotUpdateFunc);
        }
        this.realtime_update_graph(19);
      },
      realtime_update_graph: function (index)
      {
        var self = this;
        /*(index>30)
        {
            for (counter in this.counterlist)
            {
                clearInterval(this.updateSlotFunc[counter]);
            }
            chart = chart.destroy();
            this.realtime_init();
        }*/
        chart.xgrids.add([{value: index+1, text:self.currentTime[0],class:'hoge'}]);
        chart.flow({
            columns: this.NextSlot,
            duration: this.interval*1000,
            done:function(){
                if (!self.stopFlag)
                    self.realtime_update_graph(index+1);
            }
        });
      },
      update_slot : function (counter)
      {
        var machine = this.counterlist[counter]['machine'];
        
        if (this.stopFlag)
        {
            for (counter in this.counterlist)
            {
                clearInterval(this.updateSlotFunc[counter]);
                this.colorlist.$set(counter,'blue');
            }
        }

        var self = this;
        var query_cmd, query_arguments;
        if(self.counterInfo[counter].index == 0)
        {
            query_cmd = "counter." + ((self.graphtype=='bar')?'value':self.graphtype)
            query_arguments = [self.counterInfo[counter].name];
        }
        else
        {
            query_cmd = "counter." + ((self.graphtype=='bar')?'value':self.graphtype) + "i";
            query_arguments = [self.counterInfo[counter].index];
        }

        var client = new cliApp("http://" + machine);
        result = client.call({
                args: new command({
                cmd: query_cmd,
                arguments: query_arguments
            }),
            async: true,
            on_success: function (data){
                try {
                    data = JSON.parse(data);
                }
                catch(err) {
                    data = {'val':'Invalid Data','time':'Invalid Data'};
                }

                if(data['counter_name']!=self.counterInfo[counter].name)
                {
                    self.counterInfo[counter].index = 0;
                    return;
                }
                else
                    self.counterInfo[counter].index = data['counter_index'];

                self.currentValue.$set(counter, data['val']);
                self.currentTime.$set(counter, nstostr(data['time']));
                if (self.stopFlag)
                    self.colorlist.$set(counter,'blue');
                else
                    self.colorlist.$set(counter,'black');

                if(self.graphtype!='bar')
                    self.NextSlot[Number(counter)+1][1]=data['val'];
            },
            on_fail: function (xhr, textStatus, errorThrown) {
                self.colorlist.$set(counter,'red');
            }
        });
      },
  },
  ready: function () {
      this.counterlist = JSON.parse(localStorage['counterList']);
      this.graphtype = localStorage['graphtype'];
      this.interval = localStorage['interval'];
  }
})

var opButton = Vue.extend({
  template: '#opButton',
  props: ['stopFlag','graphtype','viewname','interval','counterlist'],
  methods: {
    refresh: function () {
        localStorage.setItem('graphtype', this.graphtype);
        localStorage.setItem('viewname', this.viewname);
        localStorage.setItem('interval', this.interval);
        localStorage.setItem('counterList', JSON.stringify(this.counterlist));
        
        location.reload();
    },
    stop: function () {
        this.stopFlag = 1 - this.stopFlag;
    }
  },
})

var vm = new Vue({
    el: '#app',
    data:{
        viewname: '',
        counterlist: {},
        graphtype: '',
        interval: '',
        currentValue: [],
        currentTime: [],
        stopFlag: 0,
        info: '',
        updateSlotFunc: [],
        colorlist: []
    },
    components: {
        'view-chart': viewChart,
        'op-button': opButton,
    },
    ready: function () {
        this.viewname = localStorage['viewname'];
        if (localStorage['graphtype']=='sample' || localStorage['graphtype']=='value')
            this.$refs.viewChart.realtime_init();   
        else if (this.graphtype=='bar')
            this.$refs.viewChart.bar_init();   
    },
});

