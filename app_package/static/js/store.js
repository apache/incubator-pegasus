document.getElementById("iconToUpload").onchange = function () {
    document.getElementById("iconpath").value = this.value.replace(/^.*[\\\/]/, '');
};

document.getElementById("fileToUpload").onchange = function () {
    $('#packname').val(document.forms["fileForm"]["fileToUpload"].value.replace(/^.*[\\\/]/, '').slice(0, -3));
    var flag = true;
    for(index in vm.$data.packList)
    {
        if(vm.$data.packList[index].name == $('#packname').val())
        {
            vm.$set('ifNameDuplicated', true);
            flag = false;
            break;
        }
    }
    if(flag)
        vm.$set('ifNameDuplicated', false);
    document.getElementById("filepath").value = this.value.replace(/^.*[\\\/]/, '');
};

function validateForm() {
    var fileToUpload = document.forms["fileForm"]["fileToUpload"].value;
    if (fileToUpload == null || fileToUpload == "") {
        alert("You must choose a file to upload");
        return false;
    }


    var file_name = $('#packname').val();
    if (file_name == null || file_name == "") {
        document.forms["fileForm"]["file_name"].value = fileToUpload.replace(/^.*[\\\/]/, '').slice(0, -3);
    }
    else
    {
        document.forms["fileForm"]["file_name"].value = $('#packname').val();
    }
    
    document.forms["fileForm"]["author"].value = $('#author').val();
    document.forms["fileForm"]["description"].value = $('#description').val();
    document.forms["fileForm"]["schema_info"].value = $('#schema_info_in').val();
    document.forms["fileForm"]["schema_type"].value = $('#schema_type_in').val();
    document.forms["fileForm"]["server_type"].value = $('#server_type_in').val();
    document.forms["fileForm"]["if_stateful"].value = $('#if_stateful').val();

    var clusterTypeList = [];
    for(index in vm.$data.clusterList)
    {
        var cluster = vm.$data.clusterList[index];
        if(cluster.active)
            clusterTypeList.push(cluster.name);
    }
    document.forms["fileForm"]["cluster_type"].value = JSON.stringify(clusterTypeList);

    var params_map = {};
    for(index in vm.$data.paramList)
    {
        var kv = vm.$data.paramList[index];
        params_map[kv.key] = kv.value;
    }
    document.forms["fileForm"]["parameters"].value = JSON.stringify(params_map);

    return true;
}

var vm = new Vue({
    el: '#app',
    data:{
        metaServer: '',
        commonPort: '',

        packList: [],

        detail_schema_info: '',
        detail_schema_type: '',
        detail_server_type: '',
        detail_params: {},

        app_name: '',
        partition_count: 0,
        replica_count: 0,
        success_if_exist: true,
        package_id_to_deploy: '',
        if_stateful_to_deploy: true,

        newKey: '',
        newValue: '',
        paramList: [],

        ifNameDuplicated: false,

        clusterList: [
            {name:'kubernetes', active:false},
            {name:'docker', active:false},
            {name:'bare_medal_linux', active:false},
            {name:'bare_medal_windows', active:false},
            {name:'rdsn_linux', active:false},
            {name:'rdsn_windows', active:false},
        ],
    },
    components: {
    },
    methods: {
        gotoFile: function(packname)
        {
            window.location.href = 'fileview.html?working_dir=pack/' + packname + '&root_dir=local';
        },
        showDetail: function(packname)
        {
            var self = this;
            $.post('/api/pack/detail', {id: packname
                }, function(data) {
                    data = JSON.parse(data);
                    self.detail_schema_info = data.schema_info;
                    self.detail_schema_type = data.schema_type;
                    self.detail_server_type = data.server_type;
                    self.detail_params = JSON.parse(data.parameters);
                }
            );
            $('#detail_modal').modal('show');
        },
        gotoInstance: function(packname)
        {
            window.location.href = 'service_meta.html?filterKey=' + packname;
        },
        predeploy: function(packname, if_stateful)
        {
            this.package_id_to_deploy = packname;
            this.if_stateful_to_deploy = if_stateful;
            $('#deploypack').modal('show');
        },
        deploy: function()
        {
            var self = this;
            var command = "meta.create_app ";
            var jsObj = JSON.stringify({
                req: {
                    app_name: self.app_name,
                    options: {
                        partition_count: parseInt(self.partition_count),
                        replica_count: parseInt(self.replica_count),
                        success_if_exist: (self.success_if_exist=='true')?true:false,
                        app_type: self.package_id_to_deploy,
                        is_stateful: (self.if_stateful_to_deploy=='true')?true:false,
                        package_id: self.package_id_to_deploy
                    }
                }

            });
            command += jsObj;
            $.post("http://" + self.metaServer + ":" + self.commonPort + "/api/cli", { 
                command: command
                }, function(data){ 
                    console.log(data);
                    window.location.href = 'service_meta.html';
                }
            );
        },
        remove: function(packname)
        {
            $.post("/api/pack/del", { name: packname
                }, function(data){ 
                   location.reload(false); 
                }
            );
        },

        addKV: function()
        {
            var key = this.newKey && this.newKey.trim();
            var value = this.newValue && this.newValue.trim();
            if (!key) {
                return;
            }
            this.paramList.push({key: key, value:value});
            this.newKey = '';        
            this.newValue = '';        
        },
        removeKV: function(kv)
        {
            this.paramList.$remove(kv);   
        },

        changeClusterState: function(index)
        {
            this.clusterList[index].active = !(this.clusterList[index].active);
        },

        updatePackList: function()
        {
            var self = this;
            $.post("/api/pack/load", {
                }, function(data){
                    self.packList = JSON.parse(data);
                    for(index in self.packList)
                    {
                        self.packList[index].imgsrc = "local/pack/" + self.packList[index].name + ".jpg";
                    }
                }   
            );
        }
    },
    ready: function ()
    {
        var self = this;
        $.post("/api/metaserverquery", { 
            }, function(data){ 
                self.metaServer = data.split(":")[0];
                self.commonPort = window.location.href.split("/")[2].split(":")[1];
            }
        );
        self.updatePackList();    
        setInterval(function () {
            self.updatePackList();    
        }, 1000);
    }
});


