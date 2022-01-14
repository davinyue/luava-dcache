package org.rdlinux.luava.dcache.utils;

import java.util.Collection;
import java.util.Map;

public class Assert {
    public static void notNull(Object object, String msg) {
        if (object == null) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void notEmpty(Collection<?> collection, String msg) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void notEmpty(Map<?, ?> collection, String msg) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(msg);
        }
    }
}
