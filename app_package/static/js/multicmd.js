var machineList = Vue.extend({
  template: '#machine-list',
  props: ['machines','newMachine'],
  methods: {
    add: function () {
      var value = this.newMachine && this.newMachine.trim();
	  if (!value) {
	    return;
	  }
	  this.machines.push(value);
	  this.newMachine = '';
    },

    remove: function (machine) {
	  this.machines.$remove(machine);
	}
  }
})

var save_modal_template = Vue.extend({
  template: '#save-modal-template',
  props: ['scenario_name','name','author','description','machines','cmdtext','interval','times','info'],
  methods: {
    save: function () {
        $('#save-modal').modal('hide');
        if (this.name=="")
        {
            this.info = 'Error: Name must be specified';
            $('#info-modal').modal('show');
            return;
        }
        
        var thisp = this;
        $.post("/api/scenario/save", {
                name: this.name,
                author: this.author,
                description: this.description,
                machines: JSON.stringify(this.machines),
                cmdtext: this.cmdtext,
                interval: this.interval,
                times: this.times,
            }, function(result){
                thisp.info = result;
                $('#info-modal').modal('show');
            }
        )
        .fail(function() {
            thisp.info = "Error: lost connection to the server";
            $('#info-modal').modal('show');
            return;
        });
    }
  }
})

var load_modal_template = Vue.extend({
  template: '#load-modal-template',
  props: ['scenario_name','scenarios','name','description','author','machines','cmdtext','interval','times'],
  methods: {
    load: function (scenario) {
        this.name = scenario.name;
        this.author = scenario.author;
        this.description = scenario.description;
        this.machines = scenario.machines;
        this.cmdtext = scenario.cmdtext;
        this.interval = scenario.interval,
        this.times = scenario.times,

        this.ready = true;
        $('#load-modal').modal('hide');
    },
    remove: function (scenario) {
        var thisp= this;
        $.post("/api/scenario/del", {
        name: scenario.name
        }, function(data){
            if (data == 'success')
            {
                var index = thisp.scenarios.indexOf(scenario);
                if (index > -1) {
                    thisp.scenarios.splice(index, 1);
                }
            }
        }
        );
    }
  }
})

var modal_button_template = Vue.extend({
  template: '#modal-button-template',
  props: ['scenario_name','scenarios','info'],
  methods: {
    init: function () {
        var thisp = this;
        $.post("/api/scenario/load", {
            }, function(data){
                var message = JSON.parse(data);
                thisp.scenarios = [];
                for (var i = 0; i < message.length; i++) {
                    thisp.scenarios.push({
                        name: message[i].name,
                        description: message[i].description,
                        author: message[i].author,
                        machines: JSON.parse(message[i].machines),
                        cmdtext: message[i].cmdtext,
                        interval: message[i].interval,
                        times: message[i].times,
                    });
                }
                $('#load-modal').modal('show');
            }
        )
        .fail(function() {
            thisp.info = "Error: lost connection to the server";
            $('#info-modal').modal('show');
            return;
        });
    }
  }
})

var send_req_button = Vue.extend({
  template: '#send-req-button',
  props: ['machines','cmdtext','interval','times','info'],
  data: function () {
    return {timeoutFunc: ''}
  },
  methods: {
    send: function () {
        //check parameter
        if ((this.interval!='' && isNaN(this.interval)) || (this.times!='' && this.times!='stop' && isNaN(this.times)))
        {
            this.info = 'Interval and times must be integers';
            $('#info-modal').modal('show');
            return;            
        }

        if (this.times == 'stop'){return;}

        //clear
        document.getElementById("jsontable").innerHTML = "";

        var thisp = this;
        for (machineNum in this.machines)
        {
            var machine = this.machines[machineNum];
            (function(machine){
                $.post("http://" + machine + "/api/cli", {
                    command: thisp.cmdtext
                }, function(data){
                    var resp = {};
                    try {
                        data = JSON.parse(data);
                    } catch (e){
                    }
                    resp[machine] = data;
                    var node = JsonHuman.format(resp);
                    document.getElementById("jsontable").appendChild(node);
                }
                )
                .fail(function() {
                    thisp.stop();
                    thisp.info = "Error: lost connection to the server " + machine;
                    $('#info-modal').modal('show');
                    return;
                });
            })(machine);
        }

        if (this.interval == '' || this.interval == '0'){return;}

        if (this.times == '' || this.times == '0')
        {
            this.timeoutFunc = setTimeout(function(){ thisp.send(); }, parseInt(thisp.interval)*1000);
        }
        else
        {
            this.timeoutFunc = setTimeout(function(){ thisp.send(); }, parseInt(thisp.interval)*1000);
            if (this.times == '1'){this.times = 'stop';}
            else {this.times = (parseInt(this.times)-1).toString();}
        }
    },
    stop: function () {
        clearTimeout(this.timeoutFunc);
        this.times = 'stop';
    }
  },
})

var vm = new Vue({
    el: '#app',
    data:{
        machines:[],
        scenarios:[],
        cmdtext:'',
        interval:'',
        times:'',
        name:'',
        author:'',
        description:'',
        info:''
    },
    components: {
        'machine-list': machineList,
        'save-modal-template': save_modal_template,
        'load-modal-template': load_modal_template,
        'modal-button-template': modal_button_template,
        'send-req-button': send_req_button,
    },
});

