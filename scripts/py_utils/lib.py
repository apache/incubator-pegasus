#!/usr/bin/python
#
# Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
# This source code is licensed under the Apache License Version 2.0, which
# can be found in the LICENSE file in the root directory of this source tree.

import click
import commands
import os
import json
import re

_global_verbose = False


def set_global_verbose(val):
    _global_verbose = val


def echo(message, color=None):
    click.echo(click.style(message, fg=color))


class PegasusCluster(object):
    def __init__(self, cfg_file_name=None, cluster_name=None):
        if cluster_name is None:
            self._cluster_name = os.path.basename(cfg_file_name).replace(
                "pegasus-", "").replace(".cfg", "").replace(".yaml", "")
        else:
            self._cluster_name = cluster_name
        self._shell_path = os.getenv("PEGASUS_SHELL_PATH")
        self._cfg_file_name = cfg_file_name
        if self._shell_path is None:
            echo(
                "Please configure environment variable PEGASUS_SHELL_PATH in your bashrc or zshrc",
                "red")
            exit(1)

    def print_unhealthy_partitions(self):
        if not self._load_cluster_info_if_needed():
            return
        read_unhealthy_app_count = int(
            self._list_detail["summary"]["read_unhealthy_app_count"])
        write_unhealthy_app_count = int(
            self._list_detail["summary"]["write_unhealthy_app_count"])
        if write_unhealthy_app_count > 0:
            echo("cluster is write unhealthy, write_unhealthy_app_count = " +
                 str(write_unhealthy_app_count), "red")
            return
        if read_unhealthy_app_count > 0:
            echo("cluster is read unhealthy, read_unhealthy_app_count = " +
                 str(read_unhealthy_app_count), "red")
            return

    def print_imbalanced_nodes(self):
        if not self._load_cluster_info_if_needed():
            return
        balance_operation_count = self._cluster_info["balance_operation_count"]
        total_cnt = int(balance_operation_count.split(',')[3].split('=')[1])

        primaries_per_node = {}
        for ip_port, node_info in self._nodes_detail.items():
            primary_count = int(node_info["primary_count"])
            primaries_per_node[ip_port] = primary_count
        if total_cnt != 0:
            print json.dumps(primaries_per_node, indent=4)

    def print_unsteady_meta_level(self):
        if not self._load_cluster_info_if_needed():
            return
        meta_level = self._cluster_info["meta_function_level"].strip()
        if meta_level != "steady":
            echo(meta_level, "red")

    def print_inconsistent_server_version(self):
        servers_info_str = self._run_shell("server_info").strip()
        servers_info = servers_info_str.splitlines()
        version_set = set({})
        for server in servers_info:
            version = re.match(r".*succeed:(.*), Started.*", server.strip())
            if version == None:
                continue
            version_set.add(version.group(1))
        if len(version_set) != 1:
            echo(servers_info_str, "red")

    def get_meta_port(self):
        with open(self._cfg_file_name) as cfg:
            for line in cfg.readlines():
                if line.strip().startswith("base_port"):
                    return int(line.split("=")[1])

    def get_meta_host(self):
        with open(self._cfg_file_name) as cfg:
            for line in cfg.readlines():
                if line.strip().startswith("host.0"):
                    return line.split("=")[1].strip()

    def is_valid(self):
        try:
            try_ls = self._run_shell("ls").strip()
        except Exception:
            return False
        return not "error=ERR_NETWORK_FAILURE" in try_ls

    def _load_cluster_info_if_needed(self):
        try:
            self._list_detail = json.loads(self._run_shell("ls -d -j").strip())
            self._cluster_info = json.loads(
                self._run_shell("cluster_info -j").strip())["cluster_info"]
            self._nodes_detail = json.loads(
                self._run_shell("nodes -d -j").strip())["details"]
            return True
        except Exception:
            return False

    def create_table(self, table, parts):
        create_result = self._run_shell(
            "create {} -p {}".format(table, parts)).strip()
        if "ERR_INVALID_PARAMETERS" in create_result:
            raise ValueError("failed to create table \"{}\"".format(table))

    def get_app_envs(self, table):
        envs_result = self._run_shell(
            "use {} \n get_app_envs".format(table)).strip()[len("OK\n"):]
        if "ERR_OBJECT_NOT_FOUND" in envs_result:
            raise ValueError("table {} does not exist".format(table))
        if envs_result == "":
            return None
        envs_result = self._run_shell(
            "use {} \n get_app_envs -j".format(table)).strip()[len("OK\n"):]
        return json.loads(envs_result)['app_envs']

    def set_app_envs(self, table, env_name, env_value):
        envs_result = self._run_shell(
            "use {} \n set_app_envs {} {}".format(
                table, env_name, env_value)).strip()[
            len("OK\n"):]
        if "ERR_OBJECT_NOT_FOUND" in envs_result:
            raise ValueError("table {} does not exist".format(table))

    def has_table(self, table):
        app_result = self._run_shell("app {} ".format(table)).strip()
        return "ERR_OBJECT_NOT_FOUND" not in app_result

    def _run_shell(self, args):
        """
        :param args: arguments passed to ./run.sh shell (type `string`)
        :return: shell output
        """
        global _global_verbose

        cmd = "cd {1}; echo -e \"{0}\" | ./run.sh shell -n {2}".format(
            args, self._shell_path, self._cluster_name)
        if _global_verbose:
            echo("executing command: \"{0}\"".format(cmd))

        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            raise RuntimeError("failed to execute \"{0}\": {1}".format(
                cmd, output))

        result = ""
        result_begin = False
        for line in output.splitlines():
            if line.startswith("The cluster meta list is:"):
                result_begin = True
                continue
            if line.startswith("dsn exit with code"):
                break
            if result_begin:
                result += line + "\n"
        return result

    def name(self):
        return self._cluster_name


def list_pegasus_clusters(config_path, env):
    clusters = []
    for fname in os.listdir(config_path):
        if not os.path.isfile(config_path + "/" + fname):
            continue
        if not fname.startswith("pegasus-"):
            continue
        if not env in fname:
            continue
        if not fname.endswith(".cfg") and not fname.endswith(".yaml"):
            continue
        if fname.endswith("proxy.cfg"):
            continue
        clusters.append(PegasusCluster(config_path + "/" + fname))
    return clusters
