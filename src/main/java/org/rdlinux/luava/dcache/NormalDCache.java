package org.rdlinux.luava.dcache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.rdlinux.luava.dcache.ops.COpsForValue;
import org.rdlinux.luava.dcache.topic.DeleteKeyMsg;
import org.rdlinux.luava.dcache.utils.Assert;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class NormalDCache<V> implements DCache<V> {
    private static final Logger log = LoggerFactory.getLogger(NormalDCache.class);
    private String name;
    private long timeout;
    private TimeUnit unit;
    private RedisTemplate<String, Object> redisTemplate;
    private RedissonClient redissonClient;
    private Cache<String, V> caffeineCache;
    private RTopic topic;
    /**
     * redis key前缀
     */
    private String redisKeyPrefix;
    private COpsForValue<V> opsForValue;


    protected NormalDCache(String name, long timeout, TimeUnit unit, RedisTemplate<String, Object> redisTemplate,
                           RedissonClient redissonClient) {
        this.name = name;
        this.timeout = timeout;
        this.unit = unit;
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.redisKeyPrefix = DCacheConstant.Redis_Cache_Prefix + name + ":";
        this.initCaffeineCache();
        this.initTopic();
        this.initOps();

    }

    private void initOps() {
        this.opsForValue = new COpsForValue<>(this.name, this.timeout, this.unit, this.caffeineCache,
                this.redisTemplate, this.redissonClient, this);
    }

    private void initCaffeineCache() {
        Caffeine<Object, Object> cfCBuilder = Caffeine.newBuilder().softValues().initialCapacity(8);
        if (this.timeout != -1) {
            cfCBuilder.expireAfterWrite(this.timeout, this.unit);
        }
        this.caffeineCache = cfCBuilder.build();
    }

    private void initTopic() {
        this.topic = this.redissonClient.getTopic(DCacheConstant.Redis_Topic_Prefix + this.name,
                new JsonJacksonCodec());
        this.topic.addListener(DeleteKeyMsg.class, (channel, msg) -> {
            Set<String> keys = msg.getKeys();
            log.info("dCache一级缓存同步删除keys:{}", keys);
            keys.forEach(key -> this.caffeineCache.invalidate(key));
        });
    }


    @Override
    public COpsForValue<V> opsForValue() {
        return this.opsForValue;
    }

    @Override
    public String getRedisKey(String key) {
        return this.redisKeyPrefix + key;
    }

    @Override
    public void delete(String key) {
        Assert.notNull(key, "key can not be null");
        //从redis删除key
        this.redisTemplate.delete(this.getRedisKey(key));
        //推送删除key事件
        this.topic.publish(new DeleteKeyMsg(Collections.singleton(key)));
    }

    @Override
    public void delete(Collection<String> keys) {
        Assert.notEmpty(keys, "keys can not be empty");
        Set<String> redisKeys = keys.stream().filter(Objects::nonNull).map(this::getRedisKey)
                .collect(Collectors.toSet());
        //从redis删除key
        this.redisTemplate.delete(redisKeys);
        //推送删除key事件
        this.topic.publish(new DeleteKeyMsg(new HashSet<>(keys)));
    }

    @Override
    public void delete(String... keys) {
        this.delete(Arrays.asList(keys));
    }
}
