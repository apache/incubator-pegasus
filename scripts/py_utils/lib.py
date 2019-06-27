#!/usr/bin/python
#
# Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
# This source code is licensed under the Apache License Version 2.0, which
# can be found in the LICENSE file in the root directory of this source tree.

import click
import commands
import os
import json

_global_verbose = False


def set_global_verbose(val):
    _global_verbose = val


def echo(message, color=None):
    click.echo(click.style(message, fg=color))


class PegasusCluster(object):
    def __init__(self, cfg_file_name):
        self._cluster_name = os.path.basename(cfg_file_name).replace(
            "pegasus-", "").replace(".cfg", "").replace(".yaml", "")
        self._shell_path = os.getenv("PEGASUS_SHELL_PATH")
        self._cfg_file_name = cfg_file_name
        if self._shell_path is None:
            echo(
                "Please configure environment variable PEGASUS_SHELL_PATH in your bashrc or zshrc",
                "red")
            exit(1)

    def print_unhealthy_partitions(self):
        list_detail = self._run_shell("ls -d -j").strip()

        list_detail_json = json.loads(list_detail)
        read_unhealthy_app_count = int(
            list_detail_json["summary"]["read_unhealthy_app_count"])
        write_unhealthy_app_count = int(
            list_detail_json["summary"]["write_unhealthy_app_count"])
        if write_unhealthy_app_count > 0:
            echo("cluster is write unhealthy, write_unhealthy_app_count = " +
                 str(write_unhealthy_app_count))
            return
        if read_unhealthy_app_count > 0:
            echo("cluster is read unhealthy, read_unhealthy_app_count = " +
                 str(read_unhealthy_app_count))
            return

    def print_imbalance_nodes(self):
        cluster_info = json.loads(self._run_shell("cluster_info -j").strip())
        nodes_detail = self._run_shell("nodes -d -j").strip()
        balance_operation_count = cluster_info["cluster_info"]["balance_operation_count"]
        total_cnt = int(balance_operation_count.split(',')[3].split('=')[1])

        primaries_per_node = {}
        for ip_port, node_info in json.loads(nodes_detail)["details"].items():
            primary_count = int(node_info["primary_count"])
            primaries_per_node[ip_port] = primary_count
        if total_cnt != 0:
            print json.dumps(primaries_per_node, indent=4)

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

    def _run_shell(self, args):
        """
        :param args: arguments passed to ./run.sh shell (type `string`)
        :return: shell output
        """
        global _global_verbose

        cmd = "cd {1}; echo {0} | ./run.sh shell -n {2}".format(
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
        if not fname.startswith("pegasus-" + env):
            continue
        if not fname.endswith(".cfg") and not fname.endswith(".yaml"):
            continue
        if fname.endswith("proxy.cfg"):
            continue
        clusters.append(PegasusCluster(config_path + "/" + fname))
    return clusters
