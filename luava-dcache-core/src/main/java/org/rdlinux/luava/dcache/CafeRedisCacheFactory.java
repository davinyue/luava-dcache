package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.base.CafeOpvBaseCache;
import org.rdlinux.luava.dcache.base.RedisOpvBaseCache;
import org.rdlinux.luava.dcache.msg.DeleteKeyMsg;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CafeRedisCacheFactory {
    private static final Logger log = LoggerFactory.getLogger(CafeRedisCacheFactory.class);
    /**
     * 单缓存
     */
    private final ConcurrentHashMap<String, OpvCache> cacheOpvMap = new ConcurrentHashMap<>();
    /**
     * 双缓存
     */
    private volatile ConcurrentHashMap<String, DOpvCache> dCacheOpvMap = new ConcurrentHashMap<>();
    private RedissonClient redissonClient;
    private RedisTemplate<Object, Object> redisTemplate;
    private String redisPrefix;
    private RTopic topic;

    public CafeRedisCacheFactory(RedissonClient redissonClient, RedisTemplate<Object, Object> redisTemplate,
                                 String redisPrefix) {
        this.redissonClient = redissonClient;
        this.redisTemplate = redisTemplate;
        this.redisPrefix = redisPrefix;
        this.initTopic();
    }

    private void initTopic() {
        log.info("初始化缓存一致性订阅");
        String topicName = this.redisPrefix + DCacheConstant.Redis_Prefix + ":topic:delete";
        long start = System.currentTimeMillis();
        while (true) {
            try {
                this.topic = this.redissonClient.getTopic(topicName, new JsonJacksonCodec());
                this.topic.addListener(DeleteKeyMsg.class, (channel, msg) -> {
                    try {
                        if (log.isDebugEnabled()) {
                            log.info("同步删除缓存{}的key:{}", msg.getCacheName(), msg.getKey());
                        }
                        this.dCacheOpvMap.get(msg.getCacheName()).deleteNotice(msg.getKey());
                    } catch (Exception ignore) {
                    }
                });
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() - start > 10000) {
                    throw e;
                }
                long sleep = 2;
                log.error("dcache订阅失败, 将在" + sleep + "秒后重试", e);
                try {
                    TimeUnit.SECONDS.sleep(sleep);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    /**
     * 获取弱一致性双缓存
     */
    public OpvCache getWeakConsistencyOpvDCache(String name, long timeout, TimeUnit timeUnit) {
        DOpvCache dCache = this.dCacheOpvMap.get(name);
        if (dCache == null) {
            synchronized (this) {
                dCache = this.dCacheOpvMap.get(name);
                if (dCache == null) {
                    log.info("创建获取弱一致性双缓存: {}", name);
                    CafeOpvBaseCache cafeCacheOpv = new CafeOpvBaseCache(timeUnit.toMillis(timeout));
                    RedisOpvBaseCache redisCacheOpv;
                    String redisCacheName = name;
                    if (this.redisPrefix != null && !this.redisPrefix.isEmpty()) {
                        redisCacheName = this.redisPrefix + ":" + name;
                    }
                    redisCacheOpv = new RedisOpvBaseCache(redisCacheName, this.redisTemplate);
                    dCache = new WeakConsistencyOpvCache(redisCacheName, timeUnit.toMillis(timeout),
                            this.redissonClient, cafeCacheOpv, redisCacheOpv, this.topic);
                    this.dCacheOpvMap.put(name, dCache);
                }
            }
        }
        return dCache;
    }

    /**
     * 获取redis单缓存
     */
    public OpvCache getRedisOpvCache(String name, long timeout, TimeUnit timeUnit) {
        OpvCache opvCache = this.cacheOpvMap.get(name);
        if (opvCache == null) {
            synchronized (this) {
                opvCache = this.cacheOpvMap.get(name);
                if (opvCache == null) {
                    if (this.redisPrefix != null && !this.redisPrefix.isEmpty()) {
                        opvCache = new OpvSingleCache(name, timeUnit.toMillis(timeout), this.redissonClient,
                                new RedisOpvBaseCache(this.redisPrefix + ":" + name, this.redisTemplate));
                    } else {
                        opvCache = new OpvSingleCache(name, timeUnit.toMillis(timeout), this.redissonClient,
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
    public OpvCache getCafeOpvCache(String name, long timeout, TimeUnit timeUnit) {
        OpvCache opvCache = this.cacheOpvMap.get(name);
        if (opvCache == null) {
            synchronized (this) {
                opvCache = this.cacheOpvMap.get(name);
                if (opvCache == null) {
                    long millis = timeUnit.toMillis(timeout);
                    opvCache = new OpvSingleCache(name, millis, this.redissonClient, new CafeOpvBaseCache(millis));
                    this.cacheOpvMap.put(name, opvCache);
                }
            }
        }
        return opvCache;
    }
}
