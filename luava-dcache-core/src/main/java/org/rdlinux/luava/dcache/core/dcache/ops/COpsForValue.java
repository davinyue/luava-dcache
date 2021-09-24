package org.rdlinux.luava.dcache.core.dcache.ops;

import com.github.benmanes.caffeine.cache.Cache;
import org.rdlinux.luava.dcache.core.dcache.DCache;
import org.rdlinux.luava.dcache.core.dcache.DCacheConstant;
import org.rdlinux.luava.dcache.core.dcache.utils.Assert;
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
import java.util.function.Function;
import java.util.stream.Collectors;

public class COpsForValue {
    private static final Logger log = LoggerFactory.getLogger(COpsForValue.class);
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
    private ValueOperations<String, Object> opsForValue;


    public COpsForValue(String name, long timeout, TimeUnit unit,
                        Cache<String, Object> caffeineCache,
                        RedisTemplate<String, Object> redisTemplate,
                        RedissonClient redissonClient,
                        DCache dCache) {
        this.name = name;
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
            if (log.isDebugEnabled()) {
                log.debug("定时删除过期key:{}", key);
            }
            this.caffeineCache.invalidate(key);
        }, lazyMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 获取从redis加载key的锁
     */
    private RLock getLoadFromRedisLock(String key) {
        return this.redissonClient.getLock(DCacheConstant.Load_From_Redis_Lock_Prefix + this.name + ":" + key);
    }

    /**
     * 获取从redis加载key的锁
     */
    private RLock getLoadFromCallLock(String key) {
        return this.redissonClient.getLock(DCacheConstant.Load_From_Call_Lock_Prefix + this.name + ":" + key);
    }

    /**
     * 获取
     */
    @SuppressWarnings("unchecked")
    public <V> V get(String key) {
        if (log.isDebugEnabled()) {
            log.debug("一级缓存查询, key:{}", key);
        }
        V value = (V) this.caffeineCache.getIfPresent(key);
        if (value == null) {
            String redisKey = this.dCache.getRedisKey(key);
            RLock lock = this.getLoadFromRedisLock(key);
            try {
                lock.lock();
                if (log.isDebugEnabled()) {
                    log.debug("一级缓存重查询, key:{}", key);
                }
                value = (V) this.caffeineCache.getIfPresent(key);
                if (value == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("二级缓存查询, key:{}", key);
                    }
                    value = (V) this.opsForValue.get(redisKey);
                    if (value != null) {
                        this.caffeineCache.put(key, value);
                        //定时删除一级缓存
                        Long expire = this.redisTemplate.getExpire(redisKey, TimeUnit.MILLISECONDS);
                        if (expire != null && expire > 0) {
                            if (log.isDebugEnabled()) {
                                log.debug("二级缓存数据存在过期时间, 需要定时删除一级缓存, key:{}", key);
                            }
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
     * 获取
     *
     * @param key  key值
     * @param call 当缓存中没有数据中调用该对象获取缓存值, 并将返回的额值存入缓存中
     */
    @SuppressWarnings("unchecked")
    public <V> V get(String key, Function<String, V> call) {
        V value = this.get(key);
        if (value == null) {
            RLock lock = this.getLoadFromCallLock(key);
            try {
                lock.lock();
                //从redis重新加载一次
                if (log.isDebugEnabled()) {
                    log.debug("二级缓存重查询, key:{}", key);
                }
                value = (V) this.opsForValue.get(this.dCache.getRedisKey(key));
                if (value == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("调用回调查询, key:{}", key);
                    }
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

    /**
     * 批量获取
     */
    @SuppressWarnings("unchecked")
    public <V> Map<String, V> multiGet(Collection<String> keys) {
        Assert.notEmpty(keys, "keys can not be empty");
        List<String> keyL = keys.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
        Assert.notEmpty(keyL, "valid key can not be empty");
        List<String> ndRdKey = new LinkedList<>();
        Map<String, V> ret = new HashMap<>((int) (keyL.size() / 0.75f) + 1);
        //将key从一级缓存取出, 并将一级缓存没有的key放到ndRdKey
        if (log.isDebugEnabled()) {
            log.debug("一级缓存查询, keys:{}", keyL);
        }
        keyL.forEach(key -> {
            V value = (V) this.caffeineCache.getIfPresent(key);
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
                    RLock lock = this.getLoadFromRedisLock(key);
                    lock.lock();
                    locks.add(lock);
                });
                //将需要从redis加载的key清空, 然后从新从一级缓存获取一次, 并将获取不到的key重新放入ndRdKey
                ndRdKey.clear();
                if (log.isDebugEnabled()) {
                    log.debug("一级缓存重查询, keys:{}", lockKey);
                }
                lockKey.forEach(key -> {
                    V value = (V) this.caffeineCache.getIfPresent(key);
                    if (value == null) {
                        ndRdKey.add(key);
                    } else {
                        ret.put(key, value);
                    }
                });
                if (ndRdKey.isEmpty()) {
                    return ret;
                }
                if (log.isDebugEnabled()) {
                    log.debug("二级缓存查询, keys:{}", ndRdKey);
                }
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
                                if (log.isDebugEnabled()) {
                                    log.debug("二级缓存数据存在过期时间, 需要定时删除一级缓存, key:{}", key);
                                }
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
     * 批量获取
     *
     * @param keys 键值
     * @param call 回调, 没有的键值将会回调该函数获取
     */
    @SuppressWarnings("unchecked")
    public <V> Map<String, V> multiGet(Collection<String> keys, Function<List<String>, Map<String, V>> call) {
        Map<String, V> ret = this.multiGet(keys);
        List<String> ndRdKey = new LinkedList<>();
        for (String key : keys) {
            if (ret.get(key) == null) {
                ndRdKey.add(key);
            }
        }
        if (ndRdKey.isEmpty()) {
            return ret;
        }
        ndRdKey = ndRdKey.stream().distinct().sorted().collect(Collectors.toList());
        List<RLock> locks = new LinkedList<>();
        try {
            for (String key : ndRdKey) {
                RLock lock = this.getLoadFromCallLock(key);
                lock.lock();
                locks.add(lock);
            }
            //从二级缓存重新获取
            List<String> redisKs = ndRdKey.stream().map(e -> this.dCache.getRedisKey(e))
                    .collect(Collectors.toList());
            if (log.isDebugEnabled()) {
                log.debug("二级缓存重查询, keys:{}", redisKs);
            }
            List<V> rdData = (List<V>) this.opsForValue.multiGet(redisKs);
            //再次过滤出二级缓存中没有的key
            List<String> callKeys = new LinkedList<>();
            if (rdData == null) {
                callKeys.addAll(ndRdKey);
            } else {
                for (int i = 0; i < ndRdKey.size(); i++) {
                    String key = ndRdKey.get(i);
                    V value = rdData.get(i);
                    if (value == null) {
                        callKeys.add(key);
                    } else {
                        ret.put(key, value);
                    }
                }
            }
            //调用call获取数据
            if (!callKeys.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("调用回调查询, keys:{}", callKeys);
                }
                Map<String, V> callRet = call.apply(callKeys);
                ret.putAll(callRet);
                this.multiSet(callRet);
            }
        } finally {
            locks.forEach(lock -> {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            });
        }
        return ret;
    }

    /**
     * 设置
     */
    public <V> void set(String key, V value) {
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
    public <V> void multiSet(Map<String, V> map) {
        Assert.notEmpty(map, "map can not be empty");
        this.caffeineCache.putAll(map);
        Map<String, V> redisMap = new HashMap<>((int) (map.size() / 0.75f) + 1);
        map.forEach((k, v) -> redisMap.put(this.dCache.getRedisKey(k), v));
        this.opsForValue.multiSet(redisMap);
        redisMap.forEach((k, v) -> this.redisTemplate.expire(k, this.timeoutMs
                + new Random().nextInt(1000), TimeUnit.MILLISECONDS));
    }
}
