package org.rdlinux.luava.dcache;

import org.rdlinux.luava.dcache.base.CacheValue;
import org.rdlinux.luava.dcache.base.OpvBaseCache;
import org.rdlinux.luava.dcache.msg.DeleteKeyMsg;
import org.redisson.api.RLock;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 弱一致缓存
 */
public class WeakConsistencyOpvCache extends OpvSingleCache implements DOpvCache {
    private static final Logger log = LoggerFactory.getLogger(WeakConsistencyOpvCache.class);
    /**
     * 二级缓存
     */
    private OpvBaseCache sOpvCache;
    private RedissonClient redissonClient;
    private String cacheName;
    private RTopic topic;
    private long timeout;

    /**
     * @param cacheName      缓存名称
     * @param timeout        缓存超时时间
     * @param redissonClient redissonClient
     * @param fOpvCache      一级缓存
     * @param sOpvCache      二级缓存
     * @param topic          删除通知主题
     */
    public WeakConsistencyOpvCache(String cacheName, long timeout, RedissonClient redissonClient,
                                   OpvBaseCache fOpvCache, OpvBaseCache sOpvCache, RTopic topic) {
        super(cacheName, timeout, redissonClient, fOpvCache);
        this.sOpvCache = sOpvCache;
        this.redissonClient = redissonClient;
        this.cacheName = cacheName;
        this.timeout = timeout;
        this.topic = topic;
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
        this.topic.publish(new DeleteKeyMsg<>(this.cacheName, key));
    }

    @Override
    public <Key> void deleteNotice(Key key) {
        super.delete(key);
    }
}
