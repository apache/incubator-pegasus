<?php 
$_PROG = new t_program("deploy_svc");

$_PROG->namespaces["cpp"] = "dsn.dist";

$tmp = new t_program("dsn");
$_PROG->includes[$tmp->name] = $tmp;
$tmp->namespaces["cpp"] = "dsn";

$tmp = new t_enum($_PROG, "cluster_type");
$tmp->add_value("kubernetes", 0);
$tmp->add_value("docker", 1);
$tmp->add_value("bare_medal_linux", 2);
$tmp->add_value("bare_medal_windows", 3);
$tmp->add_value("yarn_on_linux", 4);
$tmp->add_value("yarn_on_windows", 5);
$tmp->add_value("mesos_on_linux", 6);
$tmp->add_value("mesos_on_windows", 7);

$tmp = new t_enum($_PROG, "service_status");
$tmp->add_value("SS_PREPARE_RESOURCE", 0);
$tmp->add_value("SS_DEPLOYING", 1);
$tmp->add_value("SS_RUNNING", 2);
$tmp->add_value("SS_FAILOVER", 3);
$tmp->add_value("SS_FAILED", 4);
$tmp->add_value("SS_COUNT", 5);
$tmp->add_value("SS_INVALID", 6);

$tmp = new t_struct($_PROG, "deploy_request");
$tmp->add_field("package_id", "string");
$tmp->add_field("package_full_path", "string");
$tmp->add_field("package_server", "dsn.rpc_address");
$tmp->add_field("cluster_name", "string");
$tmp->add_field("name", "string");

$tmp = new t_struct($_PROG, "deploy_info");
$tmp->add_field("package_id", "string");
$tmp->add_field("name", "string");
$tmp->add_field("service_url", "string");
$tmp->add_field("error", "dsn.error_code");
$tmp->add_field("cluster", "string");
$tmp->add_field("status", "deploy_svc.service_status");

$tmp = new t_struct($_PROG, "deploy_info_list");
$tmp->add_field("services", "vector< deploy_svc.deploy_info>");

$tmp = new t_struct($_PROG, "cluster_info");
$tmp->add_field("name", "string");
$tmp->add_field("type", "deploy_svc.cluster_type");

$tmp = new t_struct($_PROG, "cluster_list");
$tmp->add_field("clusters", "vector< deploy_svc.cluster_info>");

$tmp = new t_service($_PROG, "deploy_svc");
$tmp2 = $tmp->add_function("deploy_svc.deploy_info", "deploy");
$tmp2->add_param("req", "deploy_svc.deploy_request");
$tmp2 = $tmp->add_function("dsn.error_code", "undeploy");
$tmp2->add_param("service_url", "string");
$tmp2 = $tmp->add_function("deploy_svc.deploy_info_list", "get_service_list");
$tmp2->add_param("package_id", "string");
$tmp2 = $tmp->add_function("deploy_svc.deploy_info", "get_service_info");
$tmp2->add_param("service_url", "string");
$tmp2 = $tmp->add_function("deploy_svc.cluster_list", "get_cluster_list");
$tmp2->add_param("format", "string");

?>
