package org.rdlinux.luava.dcache.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.rdlinux.luava.dcache.utils.Assert;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class CafeCacheOpv implements CacheOpv {
    private Cache<Object, Object> cache;

    /**
     * @param timeout 缓存里面key的过期时间, 单位毫秒
     */
    public CafeCacheOpv(long timeout) {
        Caffeine<Object, Object> cfCBuilder = Caffeine.newBuilder().softValues().initialCapacity(8);
        if (timeout != -1) {
            cfCBuilder.expireAfterWrite(timeout, TimeUnit.MILLISECONDS);
        }
        this.cache = cfCBuilder.build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Key, Value> CacheValue<Value> get(Key key) {
        Assert.notNull(key, "key can not be null");
        CacheValue<Value> ov = (CacheValue<Value>) this.cache.getIfPresent(key);
        if (ov != null) {
            if (ov.getExpire() == -1 || ov.getExpire() <= System.currentTimeMillis()) {
                return ov;
            } else {
                this.delete(key);
                return null;
            }
        }
        return ov;
    }

    @Override
    public <Key, Value> Map<Key, CacheValue<Value>> multiGet(Collection<Key> keys) {
        Assert.notEmpty(keys, "keys can not be empty");
        Map<Key, CacheValue<Value>> ret = new HashMap<>();
        keys.forEach(key -> {
            CacheValue<Value> value = this.get(key);
            if (value != null) {
                ret.put(key, value);
            }
        });
        return ret;
    }

    @Override
    public <Key> long getExpire(Key key) {
        return -1;
    }

    @Override
    public <Key, Value> void set(Key key, CacheValue<Value> value) {
        Assert.notNull(key, "key can not be null");
        Assert.notNull(value, "value can not be null");
        this.cache.put(key, value);
    }

    @Override
    public <Key, Value> void multiSet(Map<Key, CacheValue<Value>> kvs) {
        Assert.notEmpty(kvs, "kvs can not be null");
        kvs.forEach((k, v) -> {
            Assert.notNull(k, "key can not be null");
            Assert.notNull(v, "value can not be null");
            this.set(k, v);
        });
    }

    @Override
    public <Key> void delete(Key key) {
        this.cache.invalidate(key);
    }

    @Override
    public <Key> void multiDelete(Collection<Key> keys) {
        Assert.notNull(keys, "keys can not be null");
        keys.forEach(this::delete);
    }

    @Override
    public <Key> void multiDelete(Key[] keys) {
        Assert.notNull(keys, "keys can not be null");
        List<Key> lKeys = Arrays.asList(keys);
        this.multiDelete(lKeys);
    }
}
