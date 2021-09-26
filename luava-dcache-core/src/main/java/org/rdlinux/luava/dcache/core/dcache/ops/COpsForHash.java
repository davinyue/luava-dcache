package org.rdlinux.luava.dcache.core.dcache.ops;

import com.github.benmanes.caffeine.cache.Cache;
import org.rdlinux.luava.dcache.core.dcache.DCache;
import org.rdlinux.luava.dcache.core.dcache.DCacheConstant;
import org.rdlinux.luava.dcache.core.dcache.topic.DeleteHashKeyMsg;
import org.rdlinux.luava.dcache.core.dcache.utils.Assert;
import org.redisson.api.RLock;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private RTopic topic;


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
        this.initTopic();
    }

    private void initTopic() {
        this.topic = this.redissonClient.getTopic(DCacheConstant.Redis_Topic_Prefix + "dhk:" + this.name,
                new JsonJacksonCodec());
        this.topic.addListener(DeleteHashKeyMsg.class, (channel, msg) -> {
            Map<String, Object> hash = this.getHash(msg.getKey());
            Set<String> hashKeys = msg.getHashKeys();
            if (log.isDebugEnabled()) {
                log.info("dCache一级缓存同步删除hashKeys:{}", hashKeys);
            }
            hashKeys.forEach(hash::remove);
        });
    }

    /**
     * 获取从redis加载key的锁
     */
    private RLock getLoadFromRedisLock(String key, String hashKey) {
        return this.redissonClient.getLock(DCacheConstant.Load_From_Redis_Lock_Prefix + this.name + ":"
                + key + hashKey);
    }

    /**
     * 获取从redis加载key的锁
     */
    private RLock getLoadFromCallLock(String key, String hashKey) {
        return this.redissonClient.getLock(DCacheConstant.Load_From_Call_Lock_Prefix + this.name + ":"
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
     * 获取
     */
    @SuppressWarnings("unchecked")
    public <HV> HV get(String key, String hashKey, Function<String, HV> call) {
        HV ret = this.get(key, hashKey);
        if (ret == null) {
            RLock lock = this.getLoadFromRedisLock(key, hashKey);
            try {
                lock.lock();
                String redisKey = this.dCache.getRedisKey(key);
                ret = (HV) this.opsForHash.get(redisKey, hashKey);
                if (ret == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("调用回调查询, key:{}, hashKey:{}", key, hashKey);
                    }
                    ret = call.apply(hashKey);
                    if (ret != null) {
                        this.put(key, hashKey, ret);
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
     * 从map里面读取结果
     *
     * @param hashKeys 需要读取的key
     * @param source   需要读取的map
     * @param ret      读取结果写入对象
     * @return 不存在值的key
     */
    private <HV> List<String> getFromMap(Collection<String> hashKeys, Map<String, HV> source, Map<String, HV> ret) {
        List<String> ndRdKey = new LinkedList<>();
        for (String hk : hashKeys) {
            HV hv = source.get(hk);
            if (hv != null) {
                ret.put(hk, hv);
            } else {
                ndRdKey.add(hk);
            }
        }
        return ndRdKey;
    }

    /**
     * 批量获取
     */
    @SuppressWarnings("unchecked")
    public <HV> Map<String, HV> multiGet(String key, Collection<String> hashKeys) {
        Assert.notNull(key, "key can not be null");
        Assert.notEmpty(hashKeys, "hashKeys can not be null");
        List<String> hashKL = hashKeys.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
        Map<String, HV> hash = this.getHash(key);
        boolean hashIsEmpty = hash.isEmpty();
        Map<String, HV> ret = new HashMap<>();
        log.debug("一级缓存查询, key:{}, hashKeys:{}", key, hashKL);
        List<String> ndRdKey = this.getFromMap(hashKL, hash, ret);
        if (ndRdKey.isEmpty()) {
            return ret;
        }
        ndRdKey = ndRdKey.stream().sorted().collect(Collectors.toList());
        List<RLock> locks = new LinkedList<>();
        try {
            for (String hk : ndRdKey) {
                RLock lock = this.getLoadFromRedisLock(key, hk);
                lock.lock();
                locks.add(lock);
            }
            if (log.isDebugEnabled()) {
                log.debug("一级缓存重查询, key:{}, hashKeys:{}", key, ndRdKey);
            }
            ndRdKey = this.getFromMap(ndRdKey, hash, ret);
            if (!ndRdKey.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("二级缓存查询, key:{}, hashKeys:{}", key, ndRdKey);
                }
                List<HV> values = (List<HV>) this.opsForHash.multiGet(this.dCache.getRedisKey(key), ndRdKey);
                for (int i = 0; i < ndRdKey.size(); i++) {
                    String hk = ndRdKey.get(i);
                    HV hv = values.get(i);
                    if (hv != null) {
                        ret.put(hk, hv);
                        hash.put(hk, hv);
                    }
                }
                //定时删除一级缓存
                Long expire = this.redisTemplate.getExpire(this.dCache.getRedisKey(key),
                        TimeUnit.MILLISECONDS);
                if (expire != null && expire > 0 && hashIsEmpty) {
                    if (log.isDebugEnabled()) {
                        log.debug("二级缓存数据存在过期时间, 需要定时删除一级缓存, key:{}", key);
                    }
                    this.scheduleDeleteKey(key, expire);
                }
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
     * 批量获取
     */
    public <HV> Map<String, HV> multiGet(String key, String... hashKeys) {
        return this.multiGet(key, Arrays.asList(hashKeys));
    }

    /**
     * 批量获取
     */
    @SuppressWarnings("unchecked")
    public <HV> Map<String, HV> multiGet(String key, Collection<String> hashKeys,
                                         Function<List<String>, Map<String, HV>> call) {
        Map<String, HV> ret = this.multiGet(key, hashKeys);
        List<String> callKeys = new LinkedList<>();
        for (String hashKey : hashKeys) {
            if (!ret.containsKey(hashKey)) {
                callKeys.add(hashKey);
            }
        }
        if (callKeys.isEmpty()) {
            return ret;
        }
        callKeys = callKeys.stream().sorted().collect(Collectors.toList());
        List<RLock> locks = new LinkedList<>();
        try {
            for (String hk : callKeys) {
                RLock lock = this.getLoadFromRedisLock(key, hk);
                lock.lock();
                locks.add(lock);
            }
            if (log.isDebugEnabled()) {
                log.debug("二级缓存重查询, key:{}, hashKeys:{}", key, callKeys);
            }
            List<HV> hvs = (List<HV>) this.opsForHash.multiGet(this.dCache.getRedisKey(key), callKeys);
            List<String> needCks = new LinkedList<>();
            Map<String, Object> hash = this.getHash(key);
            for (int i = 0; i < callKeys.size(); i++) {
                String hk = callKeys.get(i);
                HV hv = hvs.get(i);
                if (hv != null) {
                    hash.put(hk, hv);
                    ret.put(hk, hv);
                } else {
                    needCks.add(hk);
                }
            }
            if (!needCks.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("调用回调查询, key:{}, hashKeys:{}", key, needCks);
                }
                Map<String, HV> clRet = call.apply(needCks);
                if (clRet != null && !clRet.isEmpty()) {
                    ret.putAll(clRet);
                    this.putAll(key, clRet);
                }
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
    public <HV> void put(String key, String hashKey, HV value) {
        Assert.notNull(key, "key can not be null");
        Assert.notNull(hashKey, "hashKey can not be null");
        Assert.notNull(value, "value can not be null");
        Map<String, Object> hash = this.getHash(key);
        hash.put(hashKey, value);
        String redisKey = this.dCache.getRedisKey(key);
        this.opsForHash.put(redisKey, hashKey, value);
        if (this.timeoutMs > 0) {
            this.redisTemplate.expire(redisKey, this.timeoutMs + new Random().nextInt(1000),
                    TimeUnit.MILLISECONDS);
        }
    }

    public <HV> void putAll(String key, Map<String, HV> hkvs) {
        Assert.notEmpty(hkvs, "hkvs can not be empty");
        Map<String, Object> hash = this.getHash(key);
        hash.putAll(hkvs);
        String redisKey = this.dCache.getRedisKey(key);
        this.opsForHash.putAll(redisKey, hash);
        if (this.timeoutMs > 0) {
            this.redisTemplate.expire(redisKey, this.timeoutMs + new Random().nextInt(1000),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 删除keys
     */
    public void delete(String key, String... hashKeys) {
        Assert.notNull(key, "key can not be null");
        Assert.notNull(hashKeys, "hashKeys can not be empty");
        //从redis删除key
        this.opsForHash.delete(this.dCache.getRedisKey(key), hashKeys);
        //推送删除key事件
        this.topic.publish(new DeleteHashKeyMsg(key, new HashSet<>(Arrays.asList(hashKeys))));
    }

    /**
     * 删除keys
     */
    public void delete(String key, Collection<String> hashKeys) {
        Assert.notEmpty(hashKeys, "hashKeys can not be empty");
        String[] ahks = new String[hashKeys.size()];
        hashKeys.toArray(ahks);
        this.delete(key, ahks);
    }
}
