// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package dsn.api;

/**
 * Created by weijiesun on 16-11-11.
 */
public interface KeyHasher {
    KeyHasher DEFAULT = new KeyHasher() {
        @Override
        public long hash(byte[] key) {
            return dsn.utils.tools.dsn_crc64(key);
        }
    };

    long hash(byte[] key);
}
