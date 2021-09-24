package org.rdlinux.luava.dcache.core.dcache.ops;

import com.github.benmanes.caffeine.cache.Cache;
import org.rdlinux.luava.dcache.core.dcache.DCache;
import org.rdlinux.luava.dcache.core.dcache.DCacheConstant;
import org.rdlinux.luava.dcache.core.dcache.utils.Assert;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class COpsForHash {
    private static final Logger log = LoggerFactory.getLogger(COpsForHash.class);
    private static final Object CREATE_HASH_LOCK = new Object();
    private DCache dCache;
    private ScheduledThreadPoolExecutor scheduledExecutor;
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
                       DCache dCache,
                       ScheduledThreadPoolExecutor scheduledExecutor) {
        this.name = name;
        this.timeoutMs = unit.toMillis(timeout);
        this.caffeineCache = caffeineCache;
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.opsForHash = this.redisTemplate.opsForHash();
        this.dCache = dCache;
        this.scheduledExecutor = scheduledExecutor;
    }

    /**
     * 获取从redis加载key的锁
     */
    private RLock getLoadFromRedisLock(String key, String hashKey) {
        return this.redissonClient.getLock(DCacheConstant.Load_From_Redis_Lock_Prefix + this.name + ":"
                + key + hashKey);
    }

    /**
     * 计划删除过期key
     *
     * @param key    需要删除的key
     * @param lazyMs 延迟多少毫秒执行
     */
    private void scheduleDeleteKey(String key, long lazyMs) {
        this.scheduledExecutor.schedule(() -> {
            if (log.isDebugEnabled()) {
                log.debug("定时删除过期key:{}", key);
            }
            this.caffeineCache.invalidate(key);
        }, lazyMs, TimeUnit.MILLISECONDS);
    }

    @SuppressWarnings("unchecked")
    private <HV> Map<String, HV> getHash(String key) {
        Assert.notNull(key, "key can not be null");
        Map<String, HV> hash = (Map<String, HV>) this.caffeineCache.getIfPresent(key);
        if (hash == null) {
            synchronized (CREATE_HASH_LOCK) {
                hash = (Map<String, HV>) this.caffeineCache.getIfPresent(key);
                if (hash == null) {
                    hash = new ConcurrentHashMap<>();
                    this.caffeineCache.put(key, hash);
                }
            }
        }
        return hash;
    }

    /**
     * 获取
     */
    @SuppressWarnings("unchecked")
    public <HV> HV get(String key, String hashKey) {
        Assert.notNull(key, "key can not be null");
        Assert.notNull(hashKey, "hashKey can not be null");
        if (log.isDebugEnabled()) {
            log.debug("一级缓存查询, key:{}, hashKey:{}", key, hashKey);
        }
        Map<String, HV> hash = this.getHash(key);
        HV ret = hash.get(hashKey);
        if (ret == null) {
            RLock lock = this.getLoadFromRedisLock(key, hashKey);
            try {
                lock.lock();
                if (log.isDebugEnabled()) {
                    log.debug("一级缓存重查询, key:{}, hashKey:{}", key, hashKey);
                }
                ret = (HV) this.getHash(key).get(hashKey);
                if (ret == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("二级缓存查询, key:{}, hashKey:{}", key, hashKey);
                    }
                    String redisKey = this.dCache.getRedisKey(key);
                    ret = (HV) this.opsForHash.get(redisKey, hashKey);
                    if (ret != null) {
                        //定时删除一级缓存
                        Long expire = this.redisTemplate.getExpire(redisKey, TimeUnit.MILLISECONDS);
                        if (expire != null && expire > 0) {
                            if (log.isDebugEnabled()) {
                                log.debug("二级缓存数据存在过期时间, 需要定时删除一级缓存, key:{}", key);
                            }
                            this.scheduleDeleteKey(key, expire);
                        }
                        hash.put(hashKey, ret);
                    }
                }
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
        return ret;
    }

    /**
     * 设置
     */
    public <HV> void set(String key, String hashKey, HV value) {
        Assert.notNull(key, "key can not be null");
        Assert.notNull(hashKey, "hashKey can not be null");
        Assert.notNull(value, "value can not be null");
        Map<String, Object> hash = this.getHash(key);
        hash.put(hashKey, value);
        String redisKey = this.dCache.getRedisKey(key);
        this.opsForHash.put(redisKey, hashKey, value);
        this.redisTemplate.expire(redisKey, this.timeoutMs + new Random().nextInt(1000),
                TimeUnit.MILLISECONDS);
    }
}
