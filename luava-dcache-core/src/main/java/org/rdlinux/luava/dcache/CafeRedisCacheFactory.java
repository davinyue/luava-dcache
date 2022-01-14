package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.base.CafeOpvBaseCache;
import org.rdlinux.luava.dcache.base.RedisOpvBaseCache;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ConcurrentHashMap;

public class CafeRedisCacheFactory {
    /**
     * 双缓存
     */
    private final ConcurrentHashMap<String, OpvCache> dCacheOpvMap = new ConcurrentHashMap<>();
    /**
     * 单缓存
     */
    private final ConcurrentHashMap<String, OpvCache> cacheOpvMap = new ConcurrentHashMap<>();
    private RedissonClient redissonClient;
    private RedisTemplate<Object, Object> redisTemplate;
    private String redisPrefix;

    public CafeRedisCacheFactory(RedissonClient redissonClient, RedisTemplate<Object, Object> redisTemplate,
                                 String redisPrefix) {
        this.redissonClient = redissonClient;
        this.redisTemplate = redisTemplate;
        this.redisPrefix = redisPrefix;
    }

    /**
     * 获取弱一致性双缓存
     */
    public OpvCache getWeakConsistencyOpvDCache(String name, long timeout) {
        OpvCache dCache = this.dCacheOpvMap.get(name);
        if (dCache == null) {
            synchronized (this) {
                dCache = this.dCacheOpvMap.get(name);
                if (dCache == null) {
                    CafeOpvBaseCache cafeCacheOpv = new CafeOpvBaseCache(timeout);
                    RedisOpvBaseCache redisCacheOpv;
                    if (this.redisPrefix != null && !this.redisPrefix.isEmpty()) {
                        redisCacheOpv = new RedisOpvBaseCache(this.redisPrefix + ":" + name, this.redisTemplate);
                    } else {
                        redisCacheOpv = new RedisOpvBaseCache(name, this.redisTemplate);
                    }
                    dCache = new WeakConsistencyOpvCache(name, timeout, this.redissonClient, cafeCacheOpv,
                            redisCacheOpv);
                    this.dCacheOpvMap.put(name, dCache);
                }
            }
        }
        return dCache;
    }

    /**
     * 获取redis单缓存
     */
    public OpvCache getRedisOpvCache(String name, long timeout) {
        OpvCache opvCache = this.cacheOpvMap.get(name);
        if (opvCache == null) {
            synchronized (this) {
                opvCache = this.cacheOpvMap.get(name);
                if (opvCache == null) {
                    if (this.redisPrefix != null && !this.redisPrefix.isEmpty()) {
                        opvCache = new OpvSingleCache(name, timeout, this.redissonClient,
                                new RedisOpvBaseCache(this.redisPrefix + ":" + name, this.redisTemplate));
                    } else {
                        opvCache = new OpvSingleCache(name, timeout, this.redissonClient,
                                new RedisOpvBaseCache(name, this.redisTemplate));
                    }
                    this.cacheOpvMap.put(name, opvCache);
                }
            }
        }
        return opvCache;
    }

    /**
     * 获取cafe单缓存
     */
    public OpvCache getCafeOpvCache(String name, long timeout) {
        OpvCache opvCache = this.cacheOpvMap.get(name);
        if (opvCache == null) {
            synchronized (this) {
                opvCache = this.cacheOpvMap.get(name);
                if (opvCache == null) {
                    opvCache = new OpvSingleCache(name, timeout, this.redissonClient, new CafeOpvBaseCache(timeout));
                    this.cacheOpvMap.put(name, opvCache);
                }
            }
        }
        return opvCache;
    }
}
