package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.ops.COpsForValue;

import java.util.Collection;

public interface DCache<V> {
    COpsForValue<V> opsForValue();

    String getRedisKey(String key);

    /**
     * 删除key
     */
    void delete(String key);

    /**
     * 批量删除key
     */
    void delete(Collection<String> keys);

    /**
     * 批量删除key
     */
    void delete(String... keys);
}
