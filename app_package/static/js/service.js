updateTable();

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results == null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function updateTable()
{
    package_id = getParameterByName("package_id")
    req = {"package_id":package_id};
    $.post("/api/cli", { 
        command:"server.service_list " + JSON.stringify(req)
        }, function(data){ 
            if(~data.indexOf('unknown command'))
            {
                alert('Error: unknown command service_list');
                return;
            } 
            data = JSON.parse(data)['services'];
            result = "";
            
            $("#serviceList tbody").empty();
            for(var service in data)
            {
                result = '<tr id="' + data[service]['name'] + '"><td>' + data[service]['name'] + '</td><td>' + data[service]['package_id'] + '</td><td>' + data[service]['cluster'] + '</td><td>' + data[service]['service_url'] + '</td><td>' + data[service]['status'] + '</td><td>' + data[service]['error'] + '</td><td><span class="glyphicon glyphicon-remove" aria-hidden="true" onclick="undeploy(\'' + data[service]['name'] + '\');"></span></td></tr>';
                $("#serviceList tbody").append(result);
            }
            window.setTimeout(function () {
                updateTable();
            }, 5000);
        }
    )
    .fail(function() {
        alert( "Error: lost connection to the server" );
        return;
    });
}

function undeploy(service_name) {
    req = {"service_name":service_name};
    $.post("/api/cli", { 
        command:"server.undeploy " + JSON.stringify(req)
        }, function(data){ 
            err = JSON.parse(data)['error'];
            location.reload(false)
       }
    )
    .fail(function() {
        alert( "Error: lost connection to the server" );
        return;
    });
}
