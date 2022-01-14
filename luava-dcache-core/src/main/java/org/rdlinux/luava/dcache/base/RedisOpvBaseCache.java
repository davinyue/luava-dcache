package org.rdlinux.luava.dcache.base;

import org.rdlinux.luava.dcache.utils.Assert;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RedisOpvBaseCache implements OpvBaseCache {
    GenericToStringSerializer<Object> keySerializer = new GenericToStringSerializer<>(Object.class);
    private RedisTemplate<Object, Object> redisTemplate;
    private String prefix;

    public RedisOpvBaseCache(String prefix, RedisTemplate<Object, Object> redisTemplate) {
        this.prefix = prefix;
        this.redisTemplate = redisTemplate;
    }

    private String getRedisKey(Object key) {
        Assert.notNull(key, "key can not be empty");
        byte[] serialize = this.keySerializer.serialize(key);
        Assert.notNull(serialize, "key序列化失败 can not be empty");
        String str = new String(serialize, StandardCharsets.UTF_8);
        if (this.prefix == null || this.prefix.isEmpty()) {
            return str;
        }
        return this.prefix + ":" + str;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Key, Value> CacheValue<Value> get(Key key) {
        Value v = (Value) this.redisTemplate.opsForValue().get(this.getRedisKey(key));
        if (v != null) {
            return new CacheValue<>(v, this.getExpire(key));
        }
        return null;
    }

    @Override
    public <Key, Value> Map<Key, CacheValue<Value>> multiGet(Collection<Key> keys) {
        Assert.notEmpty(keys, "keys can not be null");
        Map<Key, CacheValue<Value>> ret = new HashMap<>();
        keys.forEach(key -> ret.put(key, this.get(key)));
        return ret;
    }

    @Override
    public <Key> long getExpire(Key key) {
        String redisKey = this.getRedisKey(this.getRedisKey(key));
        Long expire = this.redisTemplate.getExpire(redisKey, TimeUnit.MILLISECONDS);
        //key已经过期或者不存在
        if (expire == null || expire == -2) {
            return System.currentTimeMillis() - 1000;
        }
        //key永不过期
        else if (expire == -1) {
            return expire;
        } else {
            return System.currentTimeMillis() + expire;
        }
    }

    @Override
    public <Key, Value> void set(Key key, CacheValue<Value> value) {
        Assert.notNull(key, "key can not be null");
        Assert.notNull(value, "value can not be null");
        this.redisTemplate.opsForValue().set(this.getRedisKey(key), value.getValue(), value.getExpire(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public <Key, Value> void multiSet(Map<Key, CacheValue<Value>> kvs) {
        Assert.notEmpty(kvs, "kvs can not be null");
        kvs.forEach(this::set);
    }

    @Override
    public <Key> void delete(Key key) {
        Assert.notNull(key, "key can not be null");
        this.redisTemplate.delete(this.getRedisKey(key));
    }

    @Override
    public <Key> void multiDelete(Collection<Key> keys) {
        Assert.notNull(keys, "keys can not be null");
        keys.forEach(this::delete);
    }

    @Override
    public <Key> void multiDelete(Key[] keys) {
        Assert.notNull(keys, "keys can not be null");
        for (Key key : keys) {
            this.delete(key);
        }
    }
}
