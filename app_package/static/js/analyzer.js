//parameter parsing function
function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

$(function () {
    $('.list-group.checked-list-box .list-group-item').each(function () {
        
        // Settings
        var $widget = $(this),
            $checkbox = $('<input type="checkbox" class="hidden" />'),
            color = ($widget.data('color') ? $widget.data('color') : "primary"),
            style = ($widget.data('style') == "button" ? "btn-" : "list-group-item-"),
            settings = {
                on: {
                    icon: 'glyphicon glyphicon-check'
                },
                off: {
                    icon: 'glyphicon glyphicon-unchecked'
                }
            };
            
        $widget.css('cursor', 'pointer')
        $widget.append($checkbox);

        // Event Handlers
        $widget.on('click', function () {
            $checkbox.prop('checked', !$checkbox.is(':checked'));
            $checkbox.triggerHandler('change');
            updateDisplay();
        });
        $checkbox.on('change', function () {
            updateDisplay();
        });
          

        // Actions
        function updateDisplay() {
            var isChecked = $checkbox.is(':checked');

            // Set the button's state
            $widget.data('state', (isChecked) ? "on" : "off");

            // Set the button's icon
            $widget.find('.state-icon')
                .removeClass()
                .addClass('state-icon ' + settings[$widget.data('state')].icon);

            // Update the button's color
            if (isChecked) {
                $widget.addClass(style + color + ' active');
            } else {
                $widget.removeClass(style + color + ' active');
            }
        }

        var checkedItems = {}, counter = 0;
        // Initialization
        function init() {
            
            if ($widget.data('checked') == true) {
                $checkbox.prop('checked', !$checkbox.is(':checked'));
            }
            
            updateDisplay();

            // Inject the icon if applicable
            if ($widget.find('.state-icon').length == 0) {
                $widget.prepend('<span class="state-icon ' + settings[$widget.data('state')].icon + '"></span>');
            }
        }
        init();
    });
    
    $('#get-checked-data').on('click', function(event) {
        event.preventDefault(); 
        $("#check-list-box li.active").each(function(idx, li) {
            checkedItems[counter] = $(li).text();
            counter++;
        });
        $('#display-json').html(JSON.stringify(checkedItems, null, '\t'));
    });
});

$(function () {
    $('.list-group.remove-list-box .list-group-item').each(function () {
        
        // Settings
        var $widget = $(this),
            $checkbox = $('<input type="checkbox" class="hidden" />'),
            color = ($widget.data('color') ? $widget.data('color') : "primary"),
            style = ($widget.data('style') == "button" ? "btn-" : "list-group-item-"),
            settings = {
                on: {
                    icon: 'glyphicon glyphicon-check'
                },
                off: {
                    icon: 'glyphicon glyphicon-remove'
                }
            };
            
        $widget.css('cursor', 'pointer')
        $widget.append($checkbox);

        // Event Handlers
        $widget.on('click', function () {
            $checkbox.prop('checked', !$checkbox.is(':checked'));
            $checkbox.triggerHandler('change');
            updateDisplay();
        });
        $checkbox.on('change', function () {
            updateDisplay();
        });
          

        // Actions
        function updateDisplay() {
            var isChecked = $checkbox.is(':checked');

            // Set the button's state
            $widget.data('state', (isChecked) ? "on" : "off");

            // Set the button's icon
            $widget.find('.state-icon')
                .removeClass()
                .addClass('state-icon ' + settings[$widget.data('state')].icon);

            // Set the button's state
            if (isChecked)
                $widget.remove();
        }

        // Initialization
        function init() {
            
            if ($widget.data('checked') == true) {
                $checkbox.prop('checked', !$checkbox.is(':checked'));
            }
            
            updateDisplay();

            // Inject the icon if applicable
            if ($widget.find('.state-icon').length == 0) {
                $widget.prepend('<span class="state-icon ' + settings[$widget.data('state')].icon + '"></span>');
            }
        }
        init();
    });
});

$(function () {
    $('.list-group-item.app').each(function () {
        
    });
});

var counterList = [], counterSelected = []
function SaveView() {
    var graphtype = $('input[name=graphtype]:checked').val();
    if (graphtype==undefined)
    {
        $('result-saveview').html("Error: No graph type chosen");
        return;
    }
    var interval = $('input[name=interval-num]').val();
    if (interval=='')
    {
        interval = $('input[name=interval]:checked').val();
        if (interval==undefined )
        {
            $('result-saveview').html("Error: No update interval chosen");
            return;
        }
    }
    var description = $('textarea#description').val();

    if ($("#viewname").val()=="")
    {
        $('result-saveview').html("Error: View name must be specified");
        return;
    }
    //var interval = ""
    $.post("/api/view/save", {
            name: $("#viewname").val(),
            author: $("#author").val(),
            counterList: JSON.stringify(counterList),
            description: description,
            graphtype: graphtype,
            interval: interval,
        }, function(result){
            $('result-saveview').html(result);
        }
    );
};

var viewList = {}
function LoadView() {
    $.post("/api/view/load", {
        }, function(data){
            var tableHTML = ""; 

            var message = JSON.parse(data);
	        for (var i = 0; i < message.length; i++) {
                tableHTML = tableHTML + '<tr id=' + message[i].name + '>' 
                + '<td>' + message[i].name + '</td>' 
                + '<td>' + message[i].description + '</td>'
                + '<td>' + message[i].author + '</td>'
                + '<td><span class="glyphicon glyphicon-play" onclick="PlayView(\'' + message[i].name + '\');"></span></td>'
                + '<td><span class="glyphicon glyphicon-import" onclick="ImportView(\'' + message[i].name + '\');"></span></td>'
                + '<td><span class="glyphicon glyphicon-remove" onclick="DelView(\'' + message[i].name + '\');"></span></td>'
                + '</tr>';
                viewList[message[i].name] = {description:message[i].description,author:message[i].author,counterList:message[i].counterList,graphtype:message[i].graphtype,interval:message[i].interval};
                
            }
            $("#viewList tbody > tr").empty();
            $("#viewList tbody").append(tableHTML);
            $('#viewlist').modal('show');
        }
    );
};

function DelView(name) {
    $.post("/api/view/del", {
        name:name
        }, function(data){
            if (data == 'success')
            {
                $('#viewList tr[id="'+name+'"]').remove();
                delete viewList[name];
            }
        }
    );
};

var counterAll;
function AddMachine(){
    if($("#newmachinetext").length) {return;}
    $('<li class="list-group-item machine" id="newmachineli"><input type="text" id="newmachinetext"></li>').insertBefore("#addmachinebut");
    $("#newmachinetext").change(function() {
        var machinename = $("#newmachinetext").val();
        if (machinename==''){machinename='unknown';}
        $("#newmachineli").remove();
        var newMachine = '<li class="list-group-item machine" id="' + machinename.replace(':','_') +'"><a onClick="Machine2App(\'' + machinename + '\');$(\'.\'+$(this).parent().attr(\'class\').replace(\' \',\'.\')).css(\'background\',\'white\');$(this).parent().css(\'background\',\'#99ffcc\');">' + machinename + '</a><span class="glyphicon glyphicon-remove pull-right" aria-hidden="true" onclick="$(\'#' + machinename.replace(':','_') +'\').remove();"></span></li>';
        $(newMachine).insertBefore("#addmachinebut");
    });
}

function Machine2App(machine) {
    $.post("http://" + machine + "/api/cli", {
        command: "counter.list"
        }, function(data){
            var message = JSON.parse(data);
            counterAll = message;
            $(".list-group-item.app").remove();
            for (app in message) {
                $("#appList").append('<li class="list-group-item app"><a onClick="App2Section(\'' + machine + '\',\'' + app + '\');$(\'.\'+$(this).parent().attr(\'class\').replace(\' \',\'.\')).css(\'background\',\'white\');$(this).parent().css(\'background\',\'#99ffcc\');">' + app + '</a></li>');
            }
            $(".list-group-item.section").remove();
            $(".list-group-item.counter").remove();
        }
    );
};

function App2Section(machine, app) {
    $(".list-group-item.section").remove();
    for (section in counterAll[app]) {
        $("#sectionList").append('<li class="list-group-item section"><a onClick="Section2Counter(\'' + machine + '\',\'' + app + '\',\'' + section + '\');$(\'.\'+$(this).parent().attr(\'class\').replace(\' \',\'.\')).css(\'background\',\'white\');$(this).parent().css(\'background\',\'#99ffcc\');">' + section + '</a></li>');
    }
    $(".list-group-item.counter").remove();
};

function Section2Counter(machine, app, section) {
    $(".list-group-item.counter").remove();
    for (counter in counterAll[app][section]) {
        $("#counterList").append('<li class="list-group-item counter"><a onClick="Counter2List(\'' + machine + '\',\'' + app + '\',\'' + section + '\',\'' + counterAll[app][section][counter].name + '\',' + counterAll[app][section][counter].index + ');$(\'.\'+$(this).parent().attr(\'class\').replace(\' \',\'.\')).css(\'background\',\'white\');$(this).parent().css(\'background\',\'#99ffcc\');">' + counterAll[app][section][counter].name + '</a></li>');
    }
};

function Counter2List(machine, app, section, counter, index) {
    $(".list-group.remove-list-box").append('<li class="list-group-item" id="' + machine.replace(':','_') + index + '"><span class="glyphicon glyphicon-remove" onclick="$(\'#' + machine.replace(':','_') + index + '\').remove();"></span>' + machine + ' * ' + app + ' * ' + section + ' * ' + counter+ '</li>');
    counterSelected.push({machine: machine, label: machine + ' * ' + app + ' * ' + section + ' * ' + counter, name:app + '*' + section + '*' + counter, index:index});
};

function List2List() {
    function containsCounter(obj, list) {
    var i;
    for (i = 0; i < list.length; i++) {
        if (list[i].name === obj.name && list[i].machine === obj.machine) {
            return true;
        }
    }
    return false;
    }

    $(".list-group.remove-list-box li").remove();
    for (counter in counterSelected) {
        var newCounter = {machine: counterSelected[counter].machine, label: counterSelected[counter].label, index: counterSelected[counter].index, name: counterSelected[counter].name};
        if (!containsCounter(newCounter,counterList))
        {
            counterList.push(newCounter);
            $("#counterListAll").append('<li class="list-group-item counter-main"><pre>' + counterSelected[counter].label + '<span class="glyphicon glyphicon-remove pull-right" aria-hidden="true" onclick="$(this).parent().parent().remove();counterList.splice(counterList.indexOf(' + '{machine:\'' + counterSelected[counter].machine + '\', label:\'' +  counterSelected[counter].label + '\', name:\'' +  counterSelected[counter].name + '\', index:' + counterSelected[counter].index + '}),1)"></span> </pre></li>');
        }
    }
    
    counterSelected = [];
};

function RunPerformanceView() {
    var url = "counterview.html"

    var graphtype = $('input[name=graphtype]:checked').val();
    if (graphtype==undefined)
    {
        $('result-runview').html("Error: No graph type chosen");
        $('#runviewres').modal('show');
        return;
    }
    var viewname = $("#viewname").val(); 
    if (viewname == '') {viewname = 'Unnamed View';}

    var interval = $('input[name=interval-num]').val();
    if (interval=='')
    {
        interval = $('input[name=interval]:checked').val();
        if (interval==undefined)
        {
            $('result-runview').html("Error: No update interval chosen");
            $('#runviewres').modal('show');
            return;
        }
    }
        
    localStorage.setItem('graphtype', graphtype)
    localStorage.setItem('viewname', viewname)
    localStorage.setItem('interval', interval)
    localStorage.setItem('counterList', JSON.stringify(counterList))

    window.open(url);
};



function ImportView(viewname) {
    $(".list-group-item.counter-main").remove();
    counterList=[]

    var list = JSON.parse(viewList[viewname].counterList)
    for (counter in list) {
        var counterData = list[counter];
        $("#counterListAll").append('<li class="list-group-item counter-main"> <pre>' + counterData.label + ' <span class="glyphicon glyphicon-remove pull-right" aria-hidden="true" onclick="$(this).parent().parent().remove();counterList.splice(counterList.indexOf(' + '{machine:\'' + counterData.machine + '\', name:\'' + counterData.name + '\', label:\'' + counterData.label + '\', index:' + counterData.index + '}),1)"></span></pre></li>');
        counterList.push({machine: counterData.machine, label: counterData.label, name: counterData.name, index: counterData.index});
    }
    $('input[name=graphtype][value=' + viewList[viewname].graphtype + ']').attr('checked', 'checked');

    var interval = viewList[viewname].interval;
    if (interval != undefined)
    {
        if (interval == 1 || interval == 5 || interval == 10 )
        {
            $('input[name=interval][value=' + interval + ']').attr('checked', 'checked');
        }
        else
        {
            $('input[name=interval-num]').val(interval);
        }
    }

    $('input[id=viewname]').val(viewname);
    $('input[id=author]').val(viewList[viewname].author);
    $('textarea[id=description]').val(viewList[viewname].description);
    $('#viewlist').modal('hide');
     
}

function PlayView(viewname) {

    var url = "counterview.html"

    var graphtype = viewList[viewname].graphtype;
    if (graphtype==undefined)
    {
        $('result-runview').html("Error: No graph type chosen");
        $('#runviewres').modal('show');
        return;
    }

    var interval = viewList[viewname].interval;
    if (interval=='')
    {
        interval = $('input[name=interval]:checked').val();
        if (interval==undefined)
        {
            $('result-runview').html("Error: No update interval chosen");
            $('#runviewres').modal('show');
            return;
        }
    }
    
    localStorage.setItem('graphtype', graphtype)
    localStorage.setItem('viewname', viewname)
    localStorage.setItem('interval', interval)
    localStorage.setItem('counterList', viewList[viewname].counterList)

    window.open(url);
}

var init_machine = getParameterByName("init_machine");
if(init_machine != '')
{
setTimeout( function () {
    $('#addcounter').modal('show');
    var newMachine = '<li class="list-group-item machine" id="' + init_machine.replace(':','_') +'"><a onClick="Machine2App(\'' + init_machine + '\');$(\'.\'+$(this).parent().attr(\'class\').replace(\' \',\'.\')).css(\'background\',\'white\');$(this).parent().css(\'background\',\'#99ffcc\');">' + init_machine + '</a><span class="glyphicon glyphicon-remove pull-right" aria-hidden="true" onclick="$(\'#' + init_machine.replace(':','_') +'\').remove();"></span></li>';
    $(newMachine).insertBefore("#addmachinebut");
    console.log('Init machine loaded');
},500);
}

