package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.cache.CafeCacheOpv;
import org.rdlinux.luava.dcache.cache.RedisCacheOpv;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ConcurrentHashMap;

public class CafeRedisCacheFactory {
    private final ConcurrentHashMap<String, DCacheOpv> dCacheOpvMap = new ConcurrentHashMap<>();
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
    public DCacheOpv getWeakConsistencyDCache(String name, long timeout) {
        DCacheOpv dCache = this.dCacheOpvMap.get(name);
        if (dCache == null) {
            synchronized (this) {
                dCache = this.dCacheOpvMap.get(name);
                if (dCache == null) {
                    CafeCacheOpv cafeCacheOpv = new CafeCacheOpv(timeout);
                    RedisCacheOpv redisCacheOpv;
                    if (this.redisPrefix != null && !this.redisPrefix.isEmpty()) {
                        redisCacheOpv = new RedisCacheOpv(this.redisPrefix + ":" + name, this.redisTemplate);
                    } else {
                        redisCacheOpv = new RedisCacheOpv(name, this.redisTemplate);
                    }
                    dCache = new WeakConsistencyDCacheOpv(name, timeout, this.redissonClient, cafeCacheOpv,
                            redisCacheOpv);
                    this.dCacheOpvMap.put(name, dCache);
                }
            }
        }
        return dCache;
    }
}
