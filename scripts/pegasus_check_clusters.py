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
> ./pegasus_check_clusters.py --env c3srv
"""

import os
import click

from py_utils import *


@click.command()
@click.option(
    "--env", default="", help="Env of pegasus cluster, eg. c3srv or c4tst")
@click.option('-v', '--verbose', count=True)
def main(env, verbose):
    pegasus_config_path = os.getenv("PEGASUS_CONFIG_PATH")
    if pegasus_config_path is None:
        echo(
            "Please configure environment variable PEGASUS_CONFIG_PATH in your bashrc or zshrc",
            "red")
        exit(1)
    if env != "":
        echo("env = " + env)
    set_global_verbose(verbose)
    clusters = list_pegasus_clusters(pegasus_config_path, env)
    for cluster in clusters:
        echo("=== " + cluster.name())
        try:
            cluster.print_imbalance_nodes()
            cluster.print_unhealthy_partitions()
        except RuntimeError as e:
            echo(str(e), "red")
            return
        echo("===")


if __name__ == "__main__":
    main()
