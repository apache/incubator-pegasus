#!/usr/bin/env python                                                                                                                                                                       
# -*- coding: utf-8 -*-
 
import requests
import json
import re
import sys
import os
import copy

#
# RESTful API doc: http://wiki.n.miui.com/pages/viewpage.action?pageId=66037692
# falcon ctrl api: http://dev.falcon.srv/doc/
#

# account info
serviceAccount = ""
serviceSeedMd5 = ""

###############################################################################

# global variables
falconServiceUrl = "http://falcon.srv"
pegasusScreenId = 18655
sessionId = ""
metaPort = ""
replicaPort = ""
collectorPort = ""

# return: bool
def get_service_port_by_minos(clusterName):
    minosEnv = os.environ.get("MINOS_CONFIG_FILE")
    if not isinstance(minosEnv, str) or len(minosEnv) == 0:
        print "WARN: environment variables 'MINOS_CONFIG_FILE' is not set"
        return False
    if not os.path.isfile(minosEnv):
        print "WARN: environment variables 'MINOS_CONFIG_FILE' is not valid"
        return False
    minosConfigDir = os.path.dirname(minosEnv)
    if not os.path.isdir(minosConfigDir):
        print "WARN: environment variables 'MINOS_CONFIG_FILE' is not valid"
        return False
    clusterConfigFile = minosConfigDir + "/xiaomi-config/conf/pegasus/pegasus-" + clusterName + ".cfg"
    if not os.path.isfile(clusterConfigFile):
        print "WARN: cluster config file '%s' not exist" % clusterConfigFile
        return False

    lines = [line.strip() for line in open(clusterConfigFile)]
    mode = ''
    global metaPort
    global replicaPort
    global collectorPort
    for line in lines:
        if line == '[meta]':
            mode = 'meta'
        elif line == '[replica]':
            mode = 'replica'
        elif line == '[collector]':
            mode = 'collector'
        m = re.search('^base_port *= *([0-9]+)', line)
        if m:
            basePort = int(m.group(1))
            if mode == 'meta':
                metaPort = str(basePort + 1)
            elif mode == 'replica':
                replicaPort = str(basePort + 1)
            elif mode == 'collector':
                collectorPort = str(basePort + 1)
            mode = ''

    print "INFO: metaPort = %s, replicaPort = %s, collectorPort = %s" % (metaPort, replicaPort, collectorPort)
    if metaPort == '' or replicaPort == '' or collectorPort == '':
        print "WARN: get port from cluster config file '%s' failed" % clusterConfigFile
        return False
    return True


# return: bool
def get_service_port_by_minos2(clusterName):
    minosEnv = os.environ.get("MINOS2_CONFIG_FILE")
    if not isinstance(minosEnv, str) or len(minosEnv) == 0:
        print "WARN: environment variables 'MINOS2_CONFIG_FILE' is not set"
        return False
    if not os.path.isfile(minosEnv):
        print "WARN: environment variables 'MINOS2_CONFIG_FILE' is not valid"
        return False
    minosConfigDir = os.path.dirname(minosEnv)
    if not os.path.isdir(minosConfigDir):
        print "WARN: environment variables 'MINOS2_CONFIG_FILE' is not valid"
        return False
    clusterConfigFile = minosConfigDir + "/xiaomi-config/conf/pegasus/pegasus-" + clusterName + ".yaml"
    if not os.path.isfile(clusterConfigFile):
        print "WARN: cluster config file '%s' not exist" % clusterConfigFile
        return False

    lines = [line.strip() for line in open(clusterConfigFile)]
    mode = ''
    global metaPort
    global replicaPort
    global collectorPort
    for line in lines:
        if line == 'meta:':
            mode = 'meta'
        elif line == 'replica:':
            mode = 'replica'
        elif line == 'collector:':
            mode = 'collector'
        m = re.search('^base *: *([0-9]+)', line)
        if m:
            basePort = int(m.group(1))
            if mode == 'meta':
                metaPort = str(basePort + 1)
            elif mode == 'replica':
                replicaPort = str(basePort + 1)
            elif mode == 'collector':
                collectorPort = str(basePort + 1)
            mode = ''

    print "INFO: metaPort = %s, replicaPort = %s, collectorPort = %s" % (metaPort, replicaPort, collectorPort)
    if metaPort == '' or replicaPort == '' or collectorPort == '':
        print "WARN: get port from cluster config file '%s' failed" % clusterConfigFile
        return False

    return True


# return:
def get_session_id():
    url = falconServiceUrl + "/v1.0/auth/info"
    headers = {
        "Accept": "text/plain"
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print "ERROR: get_session_id failed, status_code = %s, result:\n%s" % (r.status_code, r.text)
        sys.exit(1)

    c = r.headers['Set-Cookie']
    m = re.search('falconSessionId=([^;]+);', c)
    if m:
        global sessionId
        sessionId = m.group(1)
        print "INFO: sessionId =", sessionId
    else:
        print "ERROR: get_session_id failed, cookie not set"
        sys.exit(1)


# return:
def auth_by_misso():
    url = falconServiceUrl + "/v1.0/auth/callback/misso"
    headers = {
        "Cookie": "falconSessionId=" + sessionId,
        "Authorization": serviceAccount + ";" + serviceSeedMd5 + ";" + serviceSeedMd5
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print "ERROR: auth_by_misso failed, status_code = %s, result:\n%s" % (r.status_code, r.text)
        sys.exit(1)


# return:
def check_auth_info():
    url = falconServiceUrl + "/v1.0/auth/info"
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print "ERROR: check_auth_info failed, status_code = %s, result:\n%s" % (r.status_code, r.text)
        sys.exit(1)
    
    j = json.loads(r.text)
    if "user" not in j or j["user"] is None or "name" not in j["user"] or j["user"]["name"] != serviceAccount:
        print "ERROR: check_auth_info failed, bad json result:\n%s" % r.text
        sys.exit(1)


def login():
    get_session_id()
    auth_by_misso()
    check_auth_info()
    print "INFO: login succeed"
    

# return:
def logout():
    url = falconServiceUrl + "/v1.0/auth/logout"
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print "ERROR: logout failed, status_code = %s, result:\n%s" % (r.status_code, r.text)
        sys.exit(1)
    
    print "INFO: logout succeed"


# return: screenId
def create_screen(screenName):
    url = falconServiceUrl + "/v1.0/dashboard/screen"
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }
    req = {
        "pid" : pegasusScreenId,
        "name" : screenName
    }

    r = requests.post(url, headers=headers, data=json.dumps(req))
    if r.status_code != 200:
        print "ERROR: create_screen failed, screenName = %s, status_code = %s, result:\n%s" \
              % (screenName, r.status_code, r.text)
        sys.exit(1)
    
    j = json.loads(r.text)
    if "id" not in j:
        print "ERROR: create_screen failed, screenName = %s, bad json result\n%s" \
              % (screenName, r.text)
        sys.exit(1)
        
    screenId = j["id"]
    print "INFO: create_screen succeed, screenName = %s, screenId = %s" % (screenName, screenId)
    return screenId


# return: screenConfig
def prepare_screen_config(clusterName, screenTemplateFile, tableListFile):
    tableList = []
    lines = [line.strip() for line in open(tableListFile)]
    for line in lines:
        if len(line) > 0:
            if line in tableList:
                print "ERROR: bad table list file: duplicate table '%s'" % line
                sys.exit(1)
            tableList.append(line)
    if len(tableList) == 0:
        print "ERROR: bad table list file: should be non-empty list"
        sys.exit(1)

    jsonData = open(screenTemplateFile).read()
    screenJson = json.loads(jsonData)
    graphsJson = screenJson["graphs"]
    if not isinstance(graphsJson, list) or len(graphsJson) == 0:
        print "ERROR: bad screen template json: [graphs] should be provided as non-empty list"
        sys.exit(1)

    # resolve ${for.each.table} in title and ${table.name} in counters
    newGraphsJson = []
    titleSet = []
    for graphJson in graphsJson:
        title = graphJson["title"]
        if not isinstance(title, (str, unicode)) or len(title) == 0:
            print type(title)
            print "ERROR: bad screen template json: [graphs]: [title] should be provided as non-empty str"
            sys.exit(1)
        if title.find("${for.each.table}") != -1:
            for table in tableList:
                newTitle = title.replace("${for.each.table}", table)
                if newTitle in titleSet:
                    print "ERROR: bad screen template json: [graphs][%s]: duplicate resolved title '%s' " % (title, newTitle)
                    sys.exit(1)
                newGraphJson = copy.deepcopy(graphJson)
                counters = newGraphJson["counters"]
                if not isinstance(counters, list) or len(counters) == 0:
                    print "ERROR: bad screen template json: [graphs][%s]: [counters] should be provided as non-empty list" % title
                    sys.exit(1)
                newCounters = []
                for counter in counters:
                    if len(counter) != 0:
                        newCounter = counter.replace("${table.name}", table)
                        newCounters.append(newCounter)
                if len(newCounters) == 0:
                    print "ERROR: bad screen template json: [graphs][%s]: [counters] should be provided as non-empty list" % title
                    sys.exit(1)
                newGraphJson["counters"] = newCounters
                newGraphJson["title"] = newTitle
                newGraphsJson.append(newGraphJson)
                titleSet.append(newTitle)
        else:
            if title in titleSet:
                print "ERROR: bad screen template json: [graphs][%s]: duplicate title" % title
                sys.exit(1)
            newGraphsJson.append(graphJson)
            titleSet.append(title)

    screenConfig = []
    position = 1
    for graphJson in newGraphsJson:
        title = graphJson["title"]

        endpoints = graphJson["endpoints"]
        if not isinstance(endpoints, list) or len(endpoints) == 0:
            print "ERROR: bad screen template json: [graphs][%s]: [endpoints] should be provided as non-empty list" % title
            sys.exit(1)
        newEndpoints = []
        for endpoint in endpoints:
            if len(endpoint) != 0:
                newEndpoint = endpoint.replace("${cluster.name}", clusterName).replace("${meta.port}", metaPort)
                newEndpoint = newEndpoint.replace("${replica.port}", replicaPort).replace("${collector.port}", collectorPort)
                newEndpoints.append(newEndpoint)
        if len(newEndpoints) == 0:
            print "ERROR: bad screen template json: [graphs][%s]: [endpoints] should be provided as non-empty list" % title
            sys.exit(1)

        counters = graphJson["counters"]
        if not isinstance(counters, list) or len(counters) == 0:
            print "ERROR: bad screen template json: [graphs][%s]: [counters] should be provided as non-empty list" % title
            sys.exit(1)
        newCounters = []
        for counter in counters:
            if len(counter) != 0:
                newCounter = counter.replace("${cluster.name}", clusterName).replace("${meta.port}", metaPort)
                newCounter = newCounter.replace("${replica.port}", replicaPort).replace("${collector.port}", collectorPort)
                if newCounter.find("${for.each.table}") != -1:
                    for table in tableList:
                        newCounters.append(newCounter.replace("${for.each.table}", table))
                else:
                    newCounters.append(newCounter)
        if len(newCounters) == 0:
            print "ERROR: bad screen template json: [graphs][%s]: [counters] should be provided as non-empty list" % title
            sys.exit(1)

        graphType = graphJson["graph_type"]
        if not isinstance(graphType, (str, unicode)):
            print "ERROR: bad screen template json: [graphs][%s]: [graph_type] should be provided as str" % title
            sys.exit(1)
        if graphType != "h" and graphType != "k" and graphType != "a":
            print "ERROR: bad screen template json: [graphs][%s]: [graph_type] should be 'h' or 'k' or 'a'" % title
            sys.exit(1)

        method = graphJson["method"]
        if not isinstance(method, (str, unicode)):
            print "ERROR: bad screen template json: [graphs][%s]: [method] should be provided as str" % title
            sys.exit(1)
        if method != "" and method != "sum":
            print "ERROR: bad screen template json: [graphs][%s]: [method] should be '' or 'sum'" % title
            sys.exit(1)

        timespan = graphJson["timespan"]
        if not isinstance(timespan, int) or timespan <= 0:
            print "ERROR: bad screen template json: [graphs][%s]: [timespan] should be provided as positive int" % title
            sys.exit(1)
        
        graphConfig = {}
        graphConfig["counters"] = newCounters
        graphConfig["endpoints"] = newEndpoints
        graphConfig["falcon_tags"] = ""
        graphConfig["graph_type"] = graphType
        graphConfig["method"] = method
        graphConfig["position"] = position
        graphConfig["timespan"] = timespan
        graphConfig["title"] = title
        screenConfig.append(graphConfig)

        position += 1

    return screenConfig


# return: graphId
def create_graph(graphConfig):
    url = falconServiceUrl + "/v1.0/dashboard/graph"
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.post(url, headers=headers, data=json.dumps(graphConfig))
    if r.status_code != 200:
        print "ERROR: create_graph failed, graphTitle = \"%s\", status_code = %s, result:\n%s" \
              % (graphConfig["title"], r.status_code, r.text)
        sys.exit(1)
    
    j = json.loads(r.text)
    if "id" not in j:
        print "ERROR: create_graph failed, graphTitle = \"%s\", bad json result\n%s" \
              % (graphConfig["title"], r.text)
        sys.exit(1)
        
    graphId = j["id"]
    print "INFO: create_graph succeed, graphTitle = \"%s\", graphId = %s" \
          % (graphConfig["title"], graphId)

    # udpate graph position immediately
    graphConfig["id"] = graphId
    update_graph(graphConfig, "position")

    return graphId


# return: screen[]
def get_screens():
    url = falconServiceUrl + "/v1.0/dashboard/screen/pid/" + str(pegasusScreenId)
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print "ERROR: get_screens failed, status_code = %s, result:\n%s" % (r.status_code, r.text)
        sys.exit(1)
    
    j = json.loads(r.text)
    
    print "INFO: get_screens succeed, screenCount = %s" % len(j)
    return j


# return: graph[]
def get_screen_graphs(screenName, screenId):
    url = falconServiceUrl + "/v1.0/dashboard/graph/screen/" + str(screenId)
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print "ERROR: get_screen_graphs failed, screenName = %s, screenId = %s, status_code = %s, result:\n%s" \
              % (screenName, screenId, r.status_code, r.text)
        sys.exit(1)
    
    j = json.loads(r.text)
    
    print "INFO: get_screen_graphs succeed, screenName = %s, screenId = %s, graphCount = %s" \
          % (screenName, screenId, len(j))
    return j


# return:
def delete_graph(graphTitle, graphId):
    url = falconServiceUrl + "/v1.0/dashboard/graph/" + str(graphId)
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.delete(url, headers=headers)
    if r.status_code != 200 or r.text.find("delete success!") == -1:
        print "ERROR: delete_graph failed, graphTitle = \"%s\", graphId = %s, status_code = %s, result:\n%s" \
              % (graphTitle, graphId, r.status_code, r.text)
        sys.exit(1)
    
    print "INFO: delete_graph succeed, graphTitle = \"%s\", graphId = %s" % (graphTitle, graphId)


# return:
def update_graph(graphConfig, updateReason):
    url = falconServiceUrl + "/v1.0/dashboard/graph/" + str(graphConfig["id"])
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.put(url, headers=headers, data=json.dumps(graphConfig))
    if r.status_code != 200:
        print "ERROR: update_graph failed, graphTitle = \"%s\", graphId = %s, status_code = %s, result:\n%s" \
              % (graphConfig["title"], graphConfig["id"], r.status_code, r.text)
        sys.exit(1)
    
    j = json.loads(r.text)
    if "id" not in j:
        print "ERROR: update_graph failed, graphTitle = \"%s\", graphId = %s, bad json result\n%s" \
              % (graphConfig["title"], graphConfig["id"], r.text)
        sys.exit(1)
        
    print "INFO: update_graph succeed, graphTitle = \"%s\", graphId = %s, updateReason = \"%s changed\"" \
          % (graphConfig["title"], graphConfig["id"], updateReason)


# return: bool, reason
def is_equal(graph1, graph2):
    if graph1["title"] != graph2["title"]:
        return False, "title"
    if graph1["graph_type"] != graph2["graph_type"]:
        return False, "graph_type"
    if graph1["method"] != graph2["method"]:
        return False, "method"
    if graph1["position"] != graph2["position"]:
        return False, "position"
    if graph1["timespan"] != graph2["timespan"]:
        return False, "timespan"
    endpoints1 = graph1["endpoints"]
    endpoints2 = graph2["endpoints"]
    if len(endpoints1) != len(endpoints2):
        return False, "endpoints"
    for endpoint in endpoints1:
        if not endpoint in endpoints2:
            return False, "endpoints"
    counters1 = graph1["counters"]
    counters2 = graph2["counters"]
    if len(counters1) != len(counters2):
        return False, "counters"
    for counter in counters1:
        if not counter in counters2:
            return False, "counters"
    return True, ""


if __name__ == '__main__':
    if serviceAccount == "" or serviceSeedMd5 == "":
        print "ERROR: please set 'serviceAccount' and 'serviceSeedMd5' in %s" % sys.argv[0]
        sys.exit(1)

    if len(sys.argv) != 5:
        print "USAGE: python %s <cluster_name> <screen_template_file> <table_list_file> <create|update>" % sys.argv[0]
        sys.exit(1)

    clusterName = sys.argv[1]
    screenTemplateFile = sys.argv[2]
    tableListFile = sys.argv[3]
    operateType = sys.argv[4]

    if operateType != "create" and operateType != "update":
        print "ERROR: argv[4] should be 'create' or 'update', but '%s'" % operateType
        sys.exit(1)

    if not get_service_port_by_minos2(clusterName) and not get_service_port_by_minos(clusterName):
        print "ERROR: get service ports from minos config failed"
        sys.exit(1)

    login()

    if operateType == "create":
        screenConfig = prepare_screen_config(clusterName, screenTemplateFile, tableListFile)
        screenId = create_screen(screenName=clusterName)
        for graphConfig in screenConfig:
            graphConfig["screen_id"] = screenId
            create_graph(graphConfig)
        print "INFO: %s graphs created" % len(screenConfig)
    else: # update
        screens = get_screens()
        screenId = 0
        oldScreenConfig = None
        for screen in screens:
            if screen["name"] == clusterName:
                screenId = screen["id"]
                oldScreenConfig = get_screen_graphs(clusterName, screenId)
        if oldScreenConfig is None:
            print "ERROR: screen '%s' not exit, please create it first" % clusterName
            sys.exit(1)
        #print "INFO: old screen config:\n%s" % json.dumps(oldScreenConfig, indent=2)

        newScreenConfig = prepare_screen_config(clusterName, screenTemplateFile, tableListFile)
        #print "INFO: new screen config:\n%s" % json.dumps(newScreenConfig, indent=2)

        oldScreenMap = {}
        newScreenMap = {}
        for graph in oldScreenConfig:
            oldScreenMap[graph["title"]] = graph
        for graph in newScreenConfig:
            newScreenMap[graph["title"]] = graph
        deleteConfigList = []
        createConfigList = []
        updateConfigList = []
        for graph in oldScreenConfig:
            if not graph["title"] in newScreenMap:
                deleteConfigList.append(graph)
        for graph in newScreenConfig:
            if not graph["title"] in oldScreenMap:
                graph["screen_id"] = screenId
                createConfigList.append(graph)
            else:
                oldGraph = oldScreenMap[graph["title"]]
                equal, reason = is_equal(graph, oldGraph)
                if not equal:
                    graph["id"] = oldGraph["graph_id"]
                    graph["screen_id"] = screenId
                    updateConfigList.append((graph, reason))

        for graph in deleteConfigList:
            delete_graph(graphTitle=graph["title"], graphId=graph["graph_id"])
        for graph in createConfigList:
            create_graph(graph)
        for graph,reason in updateConfigList:
            update_graph(graph, reason)

        print "INFO: %d graphs deleted, %d graphs created, %d graphs updated" \
              % (len(deleteConfigList), len(createConfigList), len(updateConfigList))

    logout()

