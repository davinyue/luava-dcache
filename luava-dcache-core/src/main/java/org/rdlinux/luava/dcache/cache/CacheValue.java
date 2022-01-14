package org.rdlinux.luava.dcache.cache;

/**
 * 缓存值
 */
public class CacheValue<V> {
    /**
     * 值
     */
    private V value;
    /**
     * 到期时间戳(毫秒), -1为使用缓存实例过期时间
     */
    private long expire;

    public CacheValue(V value, long expire) {
        this.value = value;
        this.expire = expire;
    }

    public V getValue() {
        return this.value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public long getExpire() {
        return this.expire;
    }

    public void setExpire(long expire) {
        this.expire = expire;
    }
}
