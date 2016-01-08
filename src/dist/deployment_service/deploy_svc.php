<?php 
$_PROG = new t_program("deploy_svc");


$tmp = new t_struct($_PROG, "deploy_request");
$tmp->add_field("package_id", "string");
$tmp->add_field("package_url", "string");
$tmp->add_field("cluster_name", "string");
$tmp->add_field("name", "string");

$tmp = new t_struct($_PROG, "deploy_info");
$tmp->add_field("package_id", "string");
$tmp->add_field("name", "string");
$tmp->add_field("error", "string");
$tmp->add_field("cluster", "string");
$tmp->add_field("dsptr", "string");

$tmp = new t_struct($_PROG, "deploy_info_list");
$tmp->add_field("services", "vector< deploy_svc.deploy_info>");

$tmp = new t_struct($_PROG, "cluster_info");
$tmp->add_field("name", "string");
$tmp->add_field("type", "string");

$tmp = new t_struct($_PROG, "cluster_list");
$tmp->add_field("clusters", "vector< deploy_svc.cluster_info>");

$tmp = new t_service($_PROG, "deploy_svc");
$tmp2 = $tmp->add_function("deploy_svc.deploy_info", "deploy");
$tmp2->add_param("req", "deploy_svc.deploy_request");
$tmp2 = $tmp->add_function("string", "undeploy");
$tmp2->add_param("service_url", "string");
$tmp2 = $tmp->add_function("deploy_svc.deploy_info_list", "get_service_list");
$tmp2->add_param("format", "string");
$tmp2 = $tmp->add_function("deploy_svc.deploy_info", "get_service_info");
$tmp2->add_param("service_url", "string");
$tmp2 = $tmp->add_function("deploy_svc.cluster_list", "get_cluster_list");
$tmp2->add_param("format", "string");

?>
