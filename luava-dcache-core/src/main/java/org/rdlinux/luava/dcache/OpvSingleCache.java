package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.cache.CacheValue;
import org.rdlinux.luava.dcache.cache.OpvCache;
import org.rdlinux.luava.dcache.utils.Assert;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

/**
 * 单缓存缓存
 */
public class OpvSingleCache implements OpvDCache {
    private static final Logger log = LoggerFactory.getLogger(OpvSingleCache.class);
    private OpvCache opvCache;
    private RedissonClient redissonClient;
    private String cacheName;
    private long timeout;

    public OpvSingleCache(String cacheName, long timeout, RedissonClient redissonClient, OpvCache opvCache) {
        this.opvCache = opvCache;
        this.redissonClient = redissonClient;
        this.cacheName = cacheName;
        this.timeout = timeout;
    }


    protected String getLockKey(Object key) {
        return DCacheConstant.Redis_Lock_Prefix + this.cacheName + ":" + key;
    }

    @Override
    public <Key, Value> Value get(Key key) {
        Assert.notNull(key, "key can not be null");
        CacheValue<Value> cacheValue = this.opvCache.get(key);
        if (cacheValue != null) {
            return cacheValue.getValue();
        }
        return null;
    }

    @Override
    public <Key, Value> Value get(Key key, Function<Key, Value> call) {
        Value value = this.get(key);
        if (value == null) {
            RLock lock = this.redissonClient.getLock(this.getLockKey(key));
            try {
                lock.lock();
                value = this.get(key);
                if (value == null) {
                    log.debug("从回调获取:{}", key);
                    value = call.apply(key);
                    if (value != null) {
                        this.set(key, value);
                    }
                }
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
        return value;
    }

    @Override
    public <Key, Value> Map<Key, Value> multiGet(Collection<Key> keys) {
        Assert.notNull(keys, "keys can not be null");
        Map<Key, Value> ret = new HashMap<>();
        keys.forEach(key -> ret.put(key, this.get(key)));
        return ret;
    }

    @Override
    public <Key, Value> Map<Key, Value> multiGet(Collection<Key> keys,
                                                 Function<Collection<Key>, Map<Key, Value>> call) {
        Map<Key, Value> ret = this.multiGet(keys);
        Set<Key> noKeys = new HashSet<>();
        keys.forEach(key -> {
            if (ret.get(key) == null) {
                noKeys.add(key);
            }
        });
        if (!noKeys.isEmpty()) {
            List<RLock> locks = new LinkedList<>();
            try {
                noKeys.stream().sorted().forEach(noKey -> {
                    RLock lock = this.redissonClient.getLock(this.getLockKey(noKey));
                    lock.lock();
                    locks.add(lock);
                });
                Map<Key, Value> newRet = this.multiGet(noKeys);
                if (newRet == null) {
                    newRet = new HashMap<>();
                }
                if (!newRet.isEmpty()) {
                    ret.putAll(newRet);
                }
                Set<Key> callKeys = new HashSet<>();
                for (Key noKey : noKeys) {
                    if (newRet.get(noKey) == null) {
                        callKeys.add(noKey);
                    }
                }
                if (!callKeys.isEmpty()) {
                    log.debug("从回调获取:{}", callKeys);
                    Map<Key, Value> callRet = call.apply(callKeys);
                    if (callRet != null && !callRet.isEmpty()) {
                        this.multiSet(callRet);
                        ret.putAll(callRet);
                    }
                }
            } finally {
                locks.forEach(lock -> {
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                });
            }
        }
        return ret;
    }

    @Override
    public <Key, Value> void set(Key key, Value value) {
        Assert.notNull(key, "key can not be null");
        Assert.notNull(value, "value can not be null");
        CacheValue<Value> cacheValue = new CacheValue<>(value, this.timeout);
        this.opvCache.set(key, cacheValue);
    }

    @Override
    public <Key, Value> void multiSet(Map<Key, Value> kvs) {
        Assert.notEmpty(kvs, "kvs can not be null");
        kvs.forEach(this::set);
    }

    @Override
    public <Key> void delete(Key key) {
        Assert.notNull(key, "key can not be null");
        this.opvCache.delete(key);
    }

    @Override
    public <Key> void multiDelete(Collection<Key> keys) {
        Assert.notNull(keys, "keys can not be null");
        for (Key key : keys) {
            this.delete(key);
        }
    }

    @Override
    public <Key> void multiDelete(Key[] keys) {
        Assert.notNull(keys, "keys can not be null");
        for (Key key : keys) {
            this.delete(key);
        }
    }
}
