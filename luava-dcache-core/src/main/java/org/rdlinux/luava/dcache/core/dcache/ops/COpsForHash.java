package org.rdlinux.luava.dcache.core.dcache.ops;

import com.github.benmanes.caffeine.cache.Cache;
import org.rdlinux.luava.dcache.core.dcache.DCache;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class COpsForHash {
    private static final Logger log = LoggerFactory.getLogger(COpsForHash.class);
    private DCache dCache;
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
            new ThreadPoolExecutor.DiscardOldestPolicy());
    private String name;
    /**
     * 过期时间, 单位毫秒
     */
    private long timeoutMs;
    private RedisTemplate<String, Object> redisTemplate;
    private RedissonClient redissonClient;
    private Cache<String, Object> caffeineCache;
    private HashOperations<String, String, Object> opsForHash;


    public COpsForHash(String name, long timeout, TimeUnit unit,
                       Cache<String, Object> caffeineCache,
                       RedisTemplate<String, Object> redisTemplate,
                       RedissonClient redissonClient,
                       DCache dCache) {
        this.name = name;
        this.timeoutMs = unit.toMillis(timeout);
        this.caffeineCache = caffeineCache;
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.opsForHash = this.redisTemplate.opsForHash();
        this.dCache = dCache;
    }

    public <HV> HV get(String key, String hashKey) {
        return null;
    }
}
