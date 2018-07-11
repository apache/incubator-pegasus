// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author qinzuoyan
 *
 * This class provides a client tool to access pegasus data.
 */
public class PegasusCli {
    public static void usage() {
        System.out.println();
        System.out.println("USAGE: PegasusCli <config-path> <table-name> <op-name> ...");
        System.out.println();
        System.out.println("       <config-path> should be:");
        System.out.println("         - zookeeper path  : zk://host1:port1,host2:port2,host3:port3/path/to/config");
        System.out.println("         - local file path : file:///path/to/config");
        System.out.println("         - java resource   : resource:///path/to/config");
        System.out.println();
        System.out.println("       <op-name> should be:");
        System.out.println("         - get <hash-key> <sort-key>");
        System.out.println("         - set <hash-key> <sort-key> <value> [ttl_seconds]");
        System.out.println("         - del <hash-key> <sort-key>");
        System.out.println("         - multi_get <hash_key> [sort_key...]");
        System.out.println("         - multi_get_sort_keys <hash_key>");
        System.out.println("         - multi_set <hash_key> <sort_key> <value> [sort_key value...]");
        System.out.println("         - multi_del <hash_key> <sort_key> [sort_key...]");
        System.out.println("         - incr <hash_key> <sort_key> [increment]");
        System.out.println("         - scan <hash_key> [start_sort_key] [stop_sort_key] [max_count]");
        System.out.println("         - scan_all [max_count]");
        System.out.println();
        System.out.println("       For example:");
        System.out.println("           PegasusCli file://./pegasus.properties temp get hash_key sort_key");
        System.out.println("           PegasusCli file://./pegasus.properties temp scan hash_key '' '' 100");
        System.out.println();
    }

    public static void main(String args[]) {
        if (args.length < 3) {
            System.out.println("ERROR: invalid parameter count");
            usage();
            return;
        }
        String configPath = args[0];
        String appName = args[1];
        String opName = args[2];
        args = Arrays.copyOfRange(args, 3, args.length);
        byte[] hashKey = null;
        byte[] sortKey = null;
        byte[] value = null;
        int ttl_seconds = 0;
        long increment = 1;
        List<byte[]> sortKeys =  new ArrayList<byte[]>();
        List<Pair<byte[], byte[]>> sortKeyValuePairs = new ArrayList<Pair<byte[], byte[]>>();
        byte[] startSortKey = null;
        byte[] stopSortKey = null;
        int maxCount = Integer.MAX_VALUE;
        if (opName.equals("get") || opName.equals("del")) {
            if (args.length != 2) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            hashKey = args[0].getBytes();
            sortKey = args[1].getBytes();
        }
        else if (opName.equals("set")) {
            if (args.length != 3 && args.length != 4) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            hashKey = args[0].getBytes();
            sortKey = args[1].getBytes();
            value = args[2].getBytes();
            if (args.length > 3) {
                ttl_seconds = Integer.parseInt(args[3]);
            }
        }
        else if (opName.equals("multi_get")) {
            if (args.length < 1) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            hashKey = args[0].getBytes();
            for (int i = 1; i < args.length; ++i) {
                sortKeys.add(args[i].getBytes());
            }
        }
        else if (opName.equals("multi_get_sort_keys")) {
            if (args.length != 1) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            hashKey = args[0].getBytes();
        }
        else if (opName.equals("multi_set")) {
            if (args.length < 1 || args.length % 2 != 1) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            hashKey = args[0].getBytes();
            for (int i = 1; i < args.length; i+=2) {
                sortKeyValuePairs.add(Pair.of(args[i].getBytes(), args[i+1].getBytes()));
            }
        }
        else if (opName.equals("multi_del")) {
            if (args.length < 2) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            hashKey = args[0].getBytes();
            for (int i = 1; i < args.length; ++i) {
                sortKeys.add(args[i].getBytes());
            }
        }
        else if (opName.equals("incr")) {
            if (args.length != 2 && args.length != 3) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            hashKey = args[0].getBytes();
            sortKey = args[1].getBytes();
            if (args.length == 3) {
                increment = Long.parseLong(args[2]);
            }
        }
        else if (opName.equals("scan")) {
            if (args.length < 1 || args.length > 4) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            hashKey = args[0].getBytes();
            if (args.length > 1) {
                startSortKey = args[1].getBytes();
            }
            if (args.length > 2) {
                stopSortKey = args[2].getBytes();
            }
            if (args.length > 3) {
                maxCount = Integer.parseInt(args[3]);
            }
        }
        else if (opName.equals("scan_all")) {
            if (args.length > 1) {
                System.out.println("ERROR: invalid parameter count");
                usage();
                return;
            }
            if (args.length > 0) {
                maxCount = Integer.parseInt(args[0]);
            }
        }
        else {
            System.out.println("ERROR: invalid op-name: " + opName);
            usage();
            return;
        }
        try {
            PegasusClientInterface client = PegasusClientFactory.getSingletonClient(configPath);
            if (opName.equals("get")) {
                value = client.get(appName, hashKey, sortKey);
                if (value == null) {
                    System.out.println("Not found");
                }
                else {
                    System.out.printf("\"%s\"\n", new String(value));
                    System.out.println();
                    System.out.println("OK");
                }
            }
            else if (opName.equals("set")) {
                client.set(appName, hashKey, sortKey, value, ttl_seconds);
                System.out.println("OK");
            }
            else if (opName.equals("del")) {
                client.del(appName, hashKey, sortKey);
                System.out.println("OK");
            }
            else if (opName.equals("multi_get")) {
                boolean ret = client.multiGet(appName, hashKey, sortKeys, -1, -1, sortKeyValuePairs);
                for (Pair<byte[], byte[]> p : sortKeyValuePairs) {
                    System.out.printf("\"%s\":\"%s\"\t\"%s\"\n", new String(hashKey),
                            new String(p.getKey()), new String(p.getValue()));
                }
                if (sortKeyValuePairs.size() > 0)
                    System.out.println();
                System.out.printf("%d key-value pairs got, fetch %s.\n",
                        sortKeyValuePairs.size(), ret ? "completed" : "not completed");
            }
            else if (opName.equals("multi_get_sort_keys")) {
                boolean ret = client.multiGetSortKeys(appName, hashKey, -1, -1, sortKeys);
                for (byte[] k : sortKeys) {
                    System.out.printf("\"%s\"\n", new String(k));
                }
                if (sortKeys.size() > 0)
                    System.out.println();
                System.out.printf("%d sort keys got, fetch %s.\n",
                        sortKeys.size(), ret ? "completed" : "not completed");
            }
            else if (opName.equals("multi_set")) {
                client.multiSet(appName, hashKey, sortKeyValuePairs);
                System.out.println("OK");
            }
            else if (opName.equals("multi_del")) {
                client.multiDel(appName, hashKey, sortKeys);
                System.out.println("OK");
            }
            else if (opName.equals("incr")) {
                long new_value = client.incr(appName, hashKey, sortKey, increment);
                System.out.printf("%d\n", new_value);
                System.out.println();
                System.out.println("OK");
            }
            else if (opName.equals("scan")) {
                PegasusScannerInterface scanner = client.getScanner(appName, hashKey,
                        startSortKey, stopSortKey, new ScanOptions());
                int count = 0;
                Pair<Pair<byte[], byte[]>, byte[]> p;
                while (count < maxCount && (p = scanner.next()) != null) {
                    System.out.printf("\"%s\":\"%s\"\t\"%s\"\n", new String(p.getKey().getKey()),
                            new String(p.getKey().getValue()), new String(p.getValue()));
                    count++;
                }
                if (count > 0)
                    System.out.println();
                System.out.printf("%d key-value pairs got.\n", count);
            }
            else if (opName.equals("scan_all")) {
                List<PegasusScannerInterface> scanners = client.getUnorderedScanners(appName, 1, new ScanOptions());
                int count = 0;
                if (scanners.size() > 0) {
                    PegasusScannerInterface scanner = scanners.get(0);
                    Pair<Pair<byte[], byte[]>, byte[]> p;
                    while (count < maxCount && (p = scanner.next()) != null) {
                        System.out.printf("\"%s\":\"%s\"\t\"%s\"\n", new String(p.getKey().getKey()),
                                new String(p.getKey().getValue()), new String(p.getValue()));
                        count++;
                    }
                }
                if (count > 0)
                    System.out.println();
                System.out.printf("%d key-value pairs got.\n", count);
            }
        }
        catch (PException e) {
            e.printStackTrace();
        }
        finally {
            try {
                PegasusClientFactory.closeSingletonClient();
            }
            catch (PException e) {
                e.printStackTrace();
            }
        }
    }
}

