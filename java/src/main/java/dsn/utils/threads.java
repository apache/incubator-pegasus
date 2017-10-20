// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package dsn.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class threads {
    private static final AtomicInteger next_int = new AtomicInteger(0);
    private static final ThreadLocal<Integer> thread_id = new ThreadLocal<Integer>() {
        protected Integer initialValue() {
            return next_int.getAndIncrement();
        }
    };
    private static final ThreadLocal<Object> thread_notifier = new ThreadLocal<Object>() {
        protected Object initialValue() {
            return new Object();
        }
    };
    
    public static int get_index() {
        return thread_id.get();
    }

    public static Object get_notifier() {
        return thread_notifier.get();
    }
}
