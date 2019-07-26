#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
# This source code is licensed under the Apache License Version 2.0, which
# can be found in the LICENSE file in the root directory of this source tree.

"""
HOWTO
=====

./scripts/create_table.py --table ai_user_info \
                          --depart 云平台部-存储平台-KV系统组 \
                          --user wutao1&qinzuoyan \
                          --cluster bj1-ai \
                          --write_throttling "2000*delay*100" \
                          --partition_count 16

OR

./scripts/create_table.py -t ai_user_info \
                          -d 云平台部-存储平台-KV系统组 \
                          -u wutao1&qinzuoyan \
                          -c bj1-ai \
                          -w "2000*delay*100" \
                          -p 16

DEVLOPER GUIDE
==============

The source code is formatted using autopep8.
Ensure you have run formatter before committing changes.
```
autopep8 -i --aggressive --aggressive scripts/create_table.py
```

TODO(wutao1): automatically set write throttling according to the given
              estimated QPS on the table.
"""

import os
import click
import py_utils
import re
import json
import math


def validate_param_table(ctx, param, value):
    # TODO(wutao1): check illegal characters
    return value.encode('utf-8')


def validate_param_depart(ctx, param, value):
    return value.encode('utf-8')


def validate_param_user(ctx, param, value):
    return value.encode('utf-8')


def validate_param_cluster(ctx, param, value):
    return value.encode('utf-8')


def validate_param_partition_count(ctx, param, value):
    if value == 0:
        raise click.BadParameter("Cannot create table with 0 partition")
    if math.log(value, 2) != math.floor(math.log(value, 2)):
        raise click.BadParameter(
            "Partition count {} should be a power of 2".format(value))
    return value


def validate_param_write_throttling(ctx, param, value):
    if value == '':
        return None
    pattern = re.compile(r'^\d+\*delay\*\d+(,\d+\*reject\*\d+)?$')
    match = pattern.match(value)
    if match is not None:
        return value.encode('utf-8')
    else:
        raise click.BadParameter(
            'invalid value of throttle \'%s\'' % value)


def create_table_if_needed(cluster, table, partition_count):
    if not cluster.has_table(table):
        try:
            # TODO(wutao1): Outputs progress while polling.
            py_utils.echo("Creating table {}...".format(table))
            cluster.create_table(table, partition_count)
        except Exception as err:
            py_utils.echo(err, "red")
            exit(1)
    else:
        py_utils.echo("Success: table \"{}\" exists".format(table))


def set_business_info_if_needed(cluster, table, depart, user):
    new_business_info = "depart={},user={}".format(depart, user)
    set_app_envs_if_needed(cluster, table, 'business.info', new_business_info)


def set_write_throttling_if_needed(cluster, table, new_throttle):
    if new_throttle is None:
        return
    set_app_envs_if_needed(
        cluster, table, 'replica.write_throttling', new_throttle)


def set_app_envs_if_needed(cluster, table, env_name, new_env_value):
    py_utils.echo("New value of {}={}".format(env_name, new_env_value))
    envs = cluster.get_app_envs(table)
    if envs is not None and envs.get(env_name) is not None:
        old_env_value = envs.get(env_name).encode('utf-8')
        if old_env_value is not None:
            py_utils.echo("Old value of {}={}".format(env_name, old_env_value))
            if old_env_value == new_env_value:
                py_utils.echo("Success: {} keeps unchanged".format(env_name))
                return
    cluster.set_app_envs(table, env_name,
                         new_env_value)


def all_arguments_to_string(
        table,
        depart,
        user,
        cluster,
        partition_count,
        write_throttling):
    return json.dumps({
        'table': table,
        'depart': depart,
        'user': user,
        'cluster': cluster,
        'partition_count': partition_count,
        'write_throttling': write_throttling,
    }, sort_keys=True, indent=4, ensure_ascii=False, encoding='utf-8')


@click.command()
@click.option("--table", "-t",
              required=True,
              callback=validate_param_table,
              help="Name of the table you want to create.")
@click.option(
    "--depart", "-d",
    required=True,
    callback=validate_param_depart,
    help="Department of the table owner. If there are more than one levels of department, use '-' to concatenate them.")
@click.option(
    "--user", "-u",
    required=True,
    callback=validate_param_user,
    help="The table owner. If there are more than one owners, use '&' to concatenate them.")
@click.option("--cluster", "-c",
              required=True,
              callback=validate_param_cluster,
              help="The cluster name. Where you want to place the table.")
@click.option("--partition_count", "-p",
              callback=validate_param_partition_count,
              help="The partition count of the table. Empty means no create.",
              type=int)
@click.option(
    "--write_throttling", "-w",
    default="",
    callback=validate_param_write_throttling,
    help="{delay_qps_threshold}*delay*{delay_ms},{reject_qps_threshold}*reject*{delay_ms_before_reject}")
def main(table, depart, user, cluster, partition_count, write_throttling):
    if not click.confirm(
        "Confirm to create table:\n{}\n".format(
            all_arguments_to_string(
            table,
            depart,
            user,
            cluster,
            partition_count,
            write_throttling))):
        return
    c = py_utils.PegasusCluster(cluster_name=cluster)
    create_table_if_needed(c, table, partition_count)
    set_business_info_if_needed(c, table, depart, user)
    set_write_throttling_if_needed(c, table, write_throttling)


if __name__ == "__main__":
    main()
