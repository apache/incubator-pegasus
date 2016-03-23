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
        counterPool: [],

        machineList: [],
        appList: [],
        sectionList: [],
        counterList: [],
        counterQueue: [],
        newMachine: '',

        chosenMachine: '',
        chosenApp: '',
        chosenSection: '',
        chosenCounter: '',
        searchedCounter: '',

        viewList: [],

        graphtype: '',
        interval: '',

        counterJSON: [],

        viewName: '',
        author: '',
        description: '',

        info: '',
        
    },
    components: {
    },
    methods: {
        addMachine: function () {
            var value = this.newMachine && this.newMachine.trim();
            if (!value) {
              return;
            }
            this.machineList.push(value);
            this.newMachine = '';
        },
        Machine2App: function (machine) {
            var self = this;
            
            self.chosenMachine = machine;
            self.chosenApp = '';
            self.chosenSection = '';
            self.chosenCounter = '';
            self.appList = [];
            self.sectionList = [];
            self.counterList = [];
            $.post("http://" + machine + "/api/cli", {
                command: "counter.list"
                }, function(data){
                    self.counterJSON = JSON.parse(data);
                    for (app in self.counterJSON) {
                        self.appList.push(app);
                    }
                }
            );
        },
        App2Section: function (app) {
            var self = this;
            
            self.chosenApp = app;
            self.chosenSection = '';
            self.chosenCounter = '';
            self.sectionList = [];
            self.counterList = [];
            for (section in self.counterJSON[app]) {
                self.sectionList.push(section);
            }
        },
        Section2Counter: function (section) {
            var self = this;
            
            self.chosenSection = section;
            self.chosenCounter = '';
            self.counterList = [];
            for (counter in self.counterJSON[self.chosenApp][section]) {
                self.counterList.push(self.counterJSON[self.chosenApp][section][counter].name);
            }
        },
        Counter2Queue: function (counter) {
            var self = this;
            
            self.chosenCounter = counter;
            
            var label = self.chosenMachine + ' * ' + self.chosenApp + ' * ' + self.chosenSection + ' * ' + self.chosenCounter;
            var name = self.chosenApp + '*' + self.chosenSection + '*' + self.chosenCounter;

            var flag = false;
            for(index in self.counterQueue)
            {
                if(self.counterQueue[index].label==label)
                {
                    flag = true;
                    break;
                }
            }
            if(!flag)
                self.counterQueue.push({machine: self.chosenMachine, label: label, name: name, index:0});
        },
        Queue2Pool: function () {
            var self = this;
            
            for(c in self.counterQueue)
            {
                var flag = false;
                for(index in self.counterPool)
                {
                    if(self.counterPool[index].label==self.counterQueue[c].label)
                    {
                        flag = true;
                        break;
                    }
                }
                if(!flag)
                    self.counterPool.push(self.counterQueue[c]);
            }
            self.counterQueue = [];
        },
        removeMachine: function (machine) {
            this.machineList.$remove(machine);
        },
        removeCounterFromQueue: function (counter) {
            this.counterQueue.$remove(counter);
        },
        removeCounterFromPool: function (counter) {
            this.counterPool.$remove(counter);
        },

        runView: function () {
            var url = "counterview.html"

            if (this.graphtype=='')
            {
                this.info = "Error: No graph type chosen";
                $('#infomodal').modal('show');
                return;
            }
            if (this.viewName == '') {this.viewName = 'Unnamed View';}

            if (this.interval=='')
            {
                this.info = "Error: No update interval chosen";
                $('#infomodal').modal('show');
                return;
            }
                
            localStorage.setItem('graphtype', this.graphtype)
            localStorage.setItem('viewname', this.viewName)
            localStorage.setItem('interval', this.interval)
            localStorage.setItem('counterList', JSON.stringify(this.counterPool))

            window.open(url);
        },
        loadView: function () {
            var self = this;
            $.post("/api/view/load", {
                }, function(data){
                    self.viewList = JSON.parse(data);
                    $('#viewlist').modal('show');
                }
            );
        },
        saveView: function () {
            var self = this;

            if (this.viewName=='')
            {
                this.info = "Error: view name must be specified";
                $('#infomodal').modal('show');
                return;
            }

            $.post("/api/view/save", {
                name: self.viewName,
                author: self.author,
                counterList: JSON.stringify(self.counterPool),
                description: self.description,
                graphtype: self.graphtype,
                interval: self.interval,
            }, function(result){
                    self.info = result;
                    $('#infomodal').modal('show');
                }
            );
        },
        playView: function (view) {
            var url = "counterview.html"

            if (view.graphtype=='')
            {
                this.info = "Error: No graph type chosen";
                $('#infomodal').modal('show');
                return;
            }

            if (view.interval=='')
            {
                this.info = "Error: No update interval chosen";
                $('#infomodal').modal('show');
                return;
            }
                
            localStorage.setItem('graphtype', view.graphtype)
            localStorage.setItem('viewname', view.viewname)
            localStorage.setItem('interval', view.interval)
            localStorage.setItem('counterList', view.counterList)

            window.open(url);
            
        },
        importView: function (view) {
            this.graphtype = view.graphtype;
            this.viewName = view.name;
            this.interval = view.interval;
            this.author = view.author;
            this.description = view.description;
            this.counterPool = JSON.parse(view.counterList);
        },
        removeView: function (view) {
            var self = this;
            $.post("/api/view/del", {
                name:view.name
                }, function(data){
                    self.loadView();
                }
            );
        },
    },
    ready: function ()
    {
        var init_machine = getParameterByName("init_machine");
        if(init_machine != '')
        {
            this.machineList.push(init_machine);
            $('#addcounter').modal('show');
        }
    }
});


