#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
# This source code is licensed under the Apache License Version 2.0, which
# can be found in the LICENSE file in the root directory of this source tree.

"""
Basic usage:

./scripts/create_table.py --table ai_user_info --depart 人工智能部-小爱-XXX项目组 --user wutao1 --cluster bj1-ai --throttle "2000*delay*100" --parts 16

TODO: automatically set write throttling on the table.
"""

import os
import click
import py_utils
import re


def validate_param_table(ctx, param, value):
    # TODO(wutao1): check illegal characters
    return value


def validate_param_depart(ctx, param, value):
    return value.encode('utf-8')


def validate_param_user(ctx, param, value):
    return value.encode('utf-8')


def validate_param_cluster(ctx, param, value):
    return value


def validate_param_parts(ctx, param, value):
    if value == None:
        return
    try:
        parts = int(value)
        return parts
    except ValueError:
        raise click.BadParameter(
            'invalid value of partition count \'%s\'' % value)


def validate_param_throttle(ctx, param, value):
    if value == '':
        return None
    pattern = re.compile(r'^\d+\*delay\*\d+(,\d+\*reject\*\d+)?$')
    match = pattern.match(value)
    if match is not None:
        return value
    else:
        raise click.BadParameter(
            'invalid value of throttle \'%s\'' % value)


def create_table_if_needed(cluster, table, parts):
    if not cluster.has_table(table):
        try:
            cluster.create_table(table, parts)
        except Exception as err:
            py_utils.echo(err, "red")
            exit(1)
    else:
        py_utils.echo("Success: table \"{}\" exists".format(table))


def set_business_info_if_needed(cluster, table, depart, user):
    envs = cluster.get_app_envs(table)
    new_business_info = "depart={},user={}".format(depart, user)
    py_utils.echo("New value of business.info=" + envs['business.info'])

    if envs != None:
        py_utils.echo("Old value of business.info=" + envs['business.info'])
        business_info = envs['business.info'].split(',')
        (d, u) = (business_info[0].encode('utf8')[
            len('depart='):], business_info[1].encode('utf8')[len('user='):])
        if d == depart and u == user:
            py_utils.echo("Success: business info has been set already")
            return
    cluster.set_app_envs(table, 'business.info',
                         new_business_info)


def set_business_info_if_needed(cluster, table, depart, user):
    new_business_info = "depart={},user={}".format(depart, user)
    py_utils.echo("New value of business.info=" + new_business_info)

    envs = cluster.get_app_envs(table)
    if envs != None:
        old_business_info = envs.get('business.info')
        if old_business_info != None:
            py_utils.echo("Old value of business.info=" + old_business_info)
            if old_business_info.encode('utf-8') == new_business_info:
                py_utils.echo("Success: business info keeps unchanged")
                return

    cluster.set_app_envs(table, 'business.info',
                         new_business_info)


def set_write_throttling_if_needed(cluster, table, new_throttle):
    py_utils.echo("New value of replica.write_throttling=" + new_throttle)

    envs = cluster.get_app_envs(table)
    if envs != None:
        old_throttle = envs.get('replica.write_throttling')
        if old_throttle != None:
            py_utils.echo(
                "Old value of replica.write_throttling=" + old_throttle)
            if old_throttle == new_throttle:
                py_utils.echo(
                    "Success: throttle keeps unchanged")
                return
    cluster.set_app_envs(table, 'replica.write_throttling',
                         new_throttle)


@click.command()
@click.option("--table",
              required=True,
              callback=validate_param_table,
              help="Name of the table you want to create.")
@click.option("--depart",
              required=True,
              callback=validate_param_depart,
              help="Department of the table owner. If there are more than one levels of department, use '-' to concatenate them")
@click.option("--user",
              required=True,
              callback=validate_param_user,
              help="The table owner. If there are more than one owners, use '&' to concatenate them.")
@click.option("--cluster",
              required=True,
              callback=validate_param_cluster,
              help="The cluster name. Where you want to place the table.")
@click.option("--parts",
              callback=validate_param_parts,
              help="The partition count of the table. Empty means no create.")
@click.option("--throttle",
              default="",
              callback=validate_param_throttle,
              help="{delay_qps_threshold}*delay*{delay_ms},{reject_qps_threshold}*reject*{delay_ms_before_reject}")
def main(table, depart, user, cluster, parts, throttle):
    c = py_utils.PegasusCluster(cluster_name=cluster)
    create_table_if_needed(c, table, parts)
    set_business_info_if_needed(c, table, depart, user)
    if throttle is not None:
        set_write_throttling_if_needed(c, table, throttle)


if __name__ == "__main__":
    main()

