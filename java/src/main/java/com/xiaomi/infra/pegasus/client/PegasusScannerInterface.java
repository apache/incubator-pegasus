// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package com.xiaomi.infra.pegasus.client;

import org.apache.commons.lang3.tuple.Pair;

/**
 * @author shenyuannan
 *
 * This class provides interfaces to scan data of a specified table.
 */
public interface PegasusScannerInterface {
    /**
     * Get the next item.
     * @return item like <<hashKey, sortKey>, value>; null returned if scan completed.
     * @throws PException
     */
    public Pair<Pair<byte[], byte[]>, byte[]> next() throws PException;

    /**
     * Close the scanner. Should be called when scan completed.
     */
    public void close();
}
