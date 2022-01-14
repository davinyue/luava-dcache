package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.cache.CacheValue;
import org.rdlinux.luava.dcache.cache.OpvCache;
import org.rdlinux.luava.dcache.msg.DeleteKeyMsg;
import org.redisson.api.RLock;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 弱一致缓存
 */
public class WeakConsistencyOpvDCache extends OpvSingleCache {
    private static final Logger log = LoggerFactory.getLogger(WeakConsistencyOpvDCache.class);
    /**
     * 二级缓存
     */
    private OpvCache sOpvCache;
    private RedissonClient redissonClient;
    private String cacheName;
    private long timeout;
    private RTopic topic;

    /**
     * @param cacheName      缓存名称
     * @param timeout        缓存超时时间
     * @param redissonClient redissonClient
     * @param fOpvCache      一级缓存
     * @param sOpvCache      二级缓存
     */
    public WeakConsistencyOpvDCache(String cacheName, long timeout, RedissonClient redissonClient, OpvCache fOpvCache,
                                    OpvCache sOpvCache) {
        super(cacheName, timeout, redissonClient, fOpvCache);
        this.sOpvCache = sOpvCache;
        this.redissonClient = redissonClient;
        this.cacheName = cacheName;
        this.timeout = timeout;
        this.initTopic();
    }

    private void initTopic() {
        this.topic = this.redissonClient.getTopic(DCacheConstant.Redis_Topic_Prefix + "dk:" + this.cacheName,
                new JsonJacksonCodec());
        this.topic.addListener(DeleteKeyMsg.class, (channel, msg) -> {
            try {
                if (log.isDebugEnabled()) {
                    log.info("同步删除一级缓存keys:{}", msg.getKey());
                }
                super.delete(msg.getKey());
            } catch (Exception ignore) {
            }
        });
    }

    @Override
    public <Key, Value> Value get(Key key) {
        Value value = super.get(key);
        if (value == null) {
            RLock lock = this.redissonClient.getLock(this.getLockKey(key));
            try {
                lock.lock();
                value = super.get(key);
                if (value == null) {
                    log.debug("从二级缓存获取:{}", key);
                    CacheValue<Value> cacheValue = this.sOpvCache.get(key);
                    if (cacheValue != null) {
                        super.set(key, cacheValue.getValue());
                        value = cacheValue.getValue();
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

    @Override
    public <Key, Value> void set(Key key, Value value) {
        super.set(key, value);
        CacheValue<Value> cacheValue = new CacheValue<>(value, this.timeout);
        this.sOpvCache.set(key, cacheValue);
    }

    @Override
    public <Key> void delete(Key key) {
        super.delete(key);
        this.sOpvCache.delete(key);
        //推送删除key事件
        this.topic.publish(new DeleteKeyMsg<>(key));
    }
}
