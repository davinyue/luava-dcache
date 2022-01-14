package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.cache.CafeOpvCache;
import org.rdlinux.luava.dcache.cache.RedisOpvCache;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ConcurrentHashMap;

public class CafeRedisCacheFactory {
    private final ConcurrentHashMap<String, OpvDCache> dCacheOpvMap = new ConcurrentHashMap<>();
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
    public OpvDCache getWeakConsistencyOpvDCache(String name, long timeout) {
        OpvDCache dCache = this.dCacheOpvMap.get(name);
        if (dCache == null) {
            synchronized (this) {
                dCache = this.dCacheOpvMap.get(name);
                if (dCache == null) {
                    CafeOpvCache cafeCacheOpv = new CafeOpvCache(timeout);
                    RedisOpvCache redisCacheOpv;
                    if (this.redisPrefix != null && !this.redisPrefix.isEmpty()) {
                        redisCacheOpv = new RedisOpvCache(this.redisPrefix + ":" + name, this.redisTemplate);
                    } else {
                        redisCacheOpv = new RedisOpvCache(name, this.redisTemplate);
                    }
                    dCache = new WeakConsistencyOpvDCache(name, timeout, this.redissonClient, cafeCacheOpv,
                            redisCacheOpv);
                    this.dCacheOpvMap.put(name, dCache);
                }
            }
        }
        return dCache;
    }
}
