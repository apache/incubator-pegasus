// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc;

import com.xiaomi.infra.pegasus.tools.Tools;

/**
 * Created by weijiesun on 16-11-11.
 */
public interface KeyHasher {
    KeyHasher DEFAULT = new KeyHasher() {
        @Override
        public long hash(byte[] key) {
            return Tools.dsn_crc64(key);
        }
    };

    long hash(byte[] key);
}
