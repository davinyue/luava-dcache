package org.rdlinux.luava.dcache.base;

import java.util.Collection;
import java.util.Map;

public interface OpvBaseCache {
    <Key, Value> CacheValue<Value> get(Key key);

    <Key, Value> Map<Key, CacheValue<Value>> multiGet(Collection<Key> keys);

    /**
     * 获取key的过期时间
     */
    <Key> long getExpire(Key key);

    <Key, Value> void set(Key key, CacheValue<Value> value);

    <Key, Value> void multiSet(Map<Key, CacheValue<Value>> kvs);

    <Key> void delete(Key key);

    <Key> void multiDelete(Collection<Key> keys);

    <Key> void multiDelete(Key[] keys);
}
