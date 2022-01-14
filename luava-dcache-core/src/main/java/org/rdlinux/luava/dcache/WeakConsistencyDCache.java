package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.cache.CacheOpv;
import org.rdlinux.luava.dcache.cache.CacheValue;
import org.rdlinux.luava.dcache.msg.DeleteKeyMsg;
import org.rdlinux.luava.dcache.utils.Assert;
import org.redisson.api.RLock;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

/**
 * 弱一致缓存
 */
public class WeakConsistencyDCache implements DCache {
    private static final Logger log = LoggerFactory.getLogger(WeakConsistencyDCache.class);
    /**
     * 一级缓存
     */
    private CacheOpv fCacheOpv;
    /**
     * 二级缓存
     */
    private CacheOpv sCacheOpv;
    private RedissonClient redisson;
    private String cacheName;
    private long timeout;
    private RTopic topic;

    public WeakConsistencyDCache(String cacheName, long timeout, RedissonClient redisson, CacheOpv fCacheOpv,
                                 CacheOpv sCacheOpv) {
        this.fCacheOpv = fCacheOpv;
        this.sCacheOpv = sCacheOpv;
        this.redisson = redisson;
        this.cacheName = cacheName;
        this.timeout = timeout;
        this.initTopic();
    }

    private void initTopic() {
        this.topic = this.redisson.getTopic(DCacheConstant.Redis_Topic_Prefix + "dk:" + this.cacheName,
                new JsonJacksonCodec());
        this.topic.addListener(DeleteKeyMsg.class, (channel, msg) -> {
            try {
                if (log.isDebugEnabled()) {
                    log.info("同步删除一级缓存keys:{}", msg.getKey());
                }
                this.fCacheOpv.delete(msg.getKey());
            } catch (Exception ignore) {
            }
        });
    }

    private String getLockKey(Object key) {
        return DCacheConstant.Redis_Lock_Prefix + this.cacheName + ":" + key;
    }

    @Override
    public <Key, Value> Value get(Key key) {
        Assert.notNull(key, "key can not be null");
        CacheValue<Value> cacheValue = this.fCacheOpv.get(key);
        if (cacheValue == null) {
            RLock lock = this.redisson.getLock(this.getLockKey(key));
            try {
                lock.lock();
                cacheValue = this.fCacheOpv.get(key);
                if (cacheValue == null) {
                    log.debug("从二级缓存获取:{}", key);
                    cacheValue = this.sCacheOpv.get(key);
                    if (cacheValue != null) {
                        this.fCacheOpv.set(key, cacheValue);
                    }
                }
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
        if (cacheValue != null) {
            return cacheValue.getValue();
        }
        return null;
    }

    @Override
    public <Key, Value> Value get(Key key, Function<Key, Value> call) {
        Value value = this.get(key);
        if (value == null) {
            RLock lock = this.redisson.getLock(this.getLockKey(key));
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
                    RLock lock = this.redisson.getLock(this.getLockKey(noKey));
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
        this.fCacheOpv.set(key, cacheValue);
        this.sCacheOpv.set(key, cacheValue);
    }

    @Override
    public <Key, Value> void multiSet(Map<Key, Value> kvs) {
        Assert.notEmpty(kvs, "kvs can not be null");
        kvs.forEach(this::set);
    }

    @Override
    public <Key> void delete(Key key) {
        Assert.notNull(key, "key can not be null");
        this.fCacheOpv.delete(key);
        this.sCacheOpv.delete(key);
        //推送删除key事件
        this.topic.publish(new DeleteKeyMsg<>(key));
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
