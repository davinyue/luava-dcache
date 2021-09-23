package org.rdlinux.luava.dcache.ops;

import com.github.benmanes.caffeine.cache.Cache;
import org.rdlinux.luava.dcache.DCache;
import org.rdlinux.luava.dcache.DCacheConstant;
import org.rdlinux.luava.dcache.utils.Assert;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class COpsForValue<V> {
    private static final Logger log = LoggerFactory.getLogger(COpsForValue.class);
    private DCache<V> dCache;
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
            new ThreadPoolExecutor.DiscardOldestPolicy());
    /**
     * 过期时间, 单位毫秒
     */
    private long timeoutMs;
    private RedisTemplate<String, Object> redisTemplate;
    private RedissonClient redissonClient;
    private Cache<String, V> caffeineCache;
    private ValueOperations<String, Object> opsForValue;


    public COpsForValue(long timeout, TimeUnit unit,
                        Cache<String, V> caffeineCache,
                        RedisTemplate<String, Object> redisTemplate,
                        RedissonClient redissonClient,
                        DCache<V> dCache) {
        this.timeoutMs = unit.toMillis(timeout);
        this.caffeineCache = caffeineCache;
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.opsForValue = this.redisTemplate.opsForValue();
        this.dCache = dCache;
    }

    /**
     * 计划删除过期key
     *
     * @param key    需要删除的key
     * @param lazyMs 延迟多少毫秒执行
     */
    private void scheduleDeleteKey(String key, long lazyMs) {
        this.executor.schedule(() -> {
            log.info("定时删除过期key:{}", key);
            this.caffeineCache.invalidate(key);
        }, lazyMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 获取
     */
    @SuppressWarnings("unchecked")
    public V get(String key) {
        V value = this.caffeineCache.getIfPresent(key);
        if (value == null) {
            String redisKey = this.dCache.getRedisKey(key);
            RLock lock = this.redissonClient.getLock(DCacheConstant.Load_From_Redis_Lock_Prefix + redisKey);
            try {
                lock.lock();
                value = this.caffeineCache.getIfPresent(key);
                if (value == null) {
                    value = (V) this.opsForValue.get(redisKey);
                    if (value != null) {
                        this.caffeineCache.put(key, value);
                        //定时删除一级缓存
                        Long expire = this.redisTemplate.getExpire(redisKey, TimeUnit.MILLISECONDS);
                        if (expire != null && expire > 0) {
                            this.scheduleDeleteKey(key, expire);
                        }
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

    /**
     * 批量获取
     */
    @SuppressWarnings("unchecked")
    public Map<String, V> multiGet(Collection<String> keys) {
        Assert.notEmpty(keys, "keys can not be empty");
        List<String> keyL = keys.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
        Assert.notEmpty(keyL, "valid key can not be empty");
        List<String> ndRdKey = new LinkedList<>();
        Map<String, V> ret = new HashMap<>((int) (keyL.size() / 0.75) + 1);
        //将key从一级缓存取出, 并将一级缓存没有的key放到ndRdKey
        keyL.forEach(key -> {
            V value = this.caffeineCache.getIfPresent(key);
            if (value == null) {
                ndRdKey.add(key);
            } else {
                ret.put(key, value);
            }
        });
        //如果ndRdKey不为空
        if (!ndRdKey.isEmpty()) {
            //对需要加载的key进行排序加锁
            List<String> lockKey = ndRdKey.stream().sorted().collect(Collectors.toList());
            List<RLock> locks = new LinkedList<>();
            try {
                lockKey.forEach(key -> {
                    RLock lock = this.redissonClient.getLock(DCacheConstant.Load_From_Redis_Lock_Prefix + key);
                    lock.lock();
                    locks.add(lock);
                });
                //将需要从redis加载的key清空, 然后从新从一级缓存获取一次, 并将获取不到的key重新放入ndRdKey
                ndRdKey.clear();
                lockKey.forEach(key -> {
                    V value = this.caffeineCache.getIfPresent(key);
                    if (value == null) {
                        ndRdKey.add(key);
                    } else {
                        ret.put(key, value);
                    }
                });
                List<V> redisVs = (List<V>) this.opsForValue.multiGet(ndRdKey.stream().map(
                        e -> this.dCache.getRedisKey(e)).collect(Collectors.toList()));
                if (redisVs != null) {
                    for (int i = 0; i < ndRdKey.size(); i++) {
                        String key = ndRdKey.get(i);
                        V value = redisVs.get(i);
                        if (value != null) {
                            this.caffeineCache.put(key, value);
                            ret.put(key, value);
                            //定时删除一级缓存
                            Long expire = this.redisTemplate.getExpire(this.dCache.getRedisKey(key),
                                    TimeUnit.MILLISECONDS);
                            if (expire != null && expire > 0) {
                                this.scheduleDeleteKey(key, expire);
                            }
                        }
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

    /**
     * 设置
     */
    public void set(String key, V value) {
        this.caffeineCache.put(key, value);
        if (this.timeoutMs > 0) {
            this.opsForValue.set(this.dCache.getRedisKey(key), value, this.timeoutMs
                    + new Random().nextInt(1000), TimeUnit.MILLISECONDS);
        } else {
            this.opsForValue.set(this.dCache.getRedisKey(key), value);
        }
    }

    /**
     * 批量设置
     */
    public void multiSet(Map<String, V> map) {
        Assert.notEmpty(map, "map can not be empty");
        this.caffeineCache.putAll(map);
        Map<String, V> redisMap = new HashMap<>((int) (map.size() / 0.75) + 1);
        map.forEach((k, v) -> redisMap.put(this.dCache.getRedisKey(k), v));
        this.opsForValue.multiSet(redisMap);
        redisMap.forEach((k, v) -> this.redisTemplate.expire(k, this.timeoutMs
                + new Random().nextInt(1000), TimeUnit.MILLISECONDS));
    }
}
