#!/usr/bin/python
#
# Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
# This source code is licensed under the Apache License Version 2.0, which
# can be found in the LICENSE file in the root directory of this source tree.
"""
Basic usage:

> vim ~/.bashrc
export PYTHONPATH=$PYTHONPATH:$HOME/.local/lib/python2.7/site-packages/ 
export PEGASUS_CONFIG_PATH=$HOME/work/conf_pegasus
export PEGASUS_SHELL_PATH=$HOME/work/pegasus
> pip install --user click
> ./pegasus_check_posts.py --env c3srv
"""

import os
import click

from py_utils import *


@click.command()
@click.option("--env", help="Env of pegasus cluster, eg. c3srv or c4tst")
def main(env):
    pegasus_config_path = os.getenv("PEGASUS_CONFIG_PATH")
    if pegasus_config_path is None:
        echo(
            "Please configure environment variable PEGASUS_CONFIG_PATH in your bashrc or zshrc",
            "red")
        exit(1)
    clusters = list_pegasus_clusters(pegasus_config_path, env)
    host_to_ports = {}
    for cluster in clusters:
        try:
            p = cluster.get_meta_port()
            h = cluster.get_meta_host()
            if not h in host_to_ports:
                host_to_ports[h] = set()
            if p in host_to_ports[h]:
                echo(
                    "port number conflicted: {0} {1} [{2}]".format(
                        p, cluster.name(), h), "red")
                continue
            host_to_ports[h].add(p)
            echo("cluster {0}: {1} [{2}]".format(cluster.name(), p, h))
        except RuntimeError as e:
            echo(str(e), "red")
            return

    echo("")
    for h in host_to_ports:
        echo("recommended port number for [{0}] is: {1}".format(
            h, str(max(host_to_ports[h]) + 1000)))
        echo("host [{0}] has in total {1} clusters on it".format(
            h, len(host_to_ports[h])))
        echo("")


if __name__ == "__main__":
    main()
