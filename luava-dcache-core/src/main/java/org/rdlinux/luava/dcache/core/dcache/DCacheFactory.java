package org.rdlinux.luava.dcache.core.dcache;

import org.rdlinux.luava.dcache.core.dcache.utils.Assert;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DCacheFactory {
    private static final Object CACHE_CREATE_LOCK = new Object();
    private final ConcurrentHashMap<String, DCache> caches = new ConcurrentHashMap<>();
    private RedisTemplate<String, Object> redisTemplate;
    private RedissonClient redissonClient;

    public DCacheFactory(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient) {
        Assert.notNull(redisTemplate, "redisTemplate can not be null");
        Assert.notNull(redissonClient, "redissonClient can not be null");
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
    }

    public DCache getCache(String name) {
        return this.getCache(name, -1);
    }

    public DCache getCache(String name, long timeout) {
        return this.getCache(name, timeout, TimeUnit.SECONDS);
    }

    public DCache getCache(String name, long timeout, TimeUnit unit) {
        Assert.notNull(name, "name can not be null");
        Assert.notNull(unit, "unit can not be null");
        if (timeout == 0) {
            timeout = -1;
        }
        DCache dCache = this.caches.get(name);
        if (dCache == null) {
            synchronized (CACHE_CREATE_LOCK) {
                dCache = this.caches.get(name);
                if (dCache == null) {
                    dCache = new NormalDCache(name, timeout, unit, this.redisTemplate, this.redissonClient);
                    this.caches.put(name, dCache);
                }
            }
        }
        return dCache;
    }
}
