package org.rdlinux.luava.dcache.test;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import org.rdlinux.luava.dcache.core.dcache.DCache;
import org.rdlinux.luava.dcache.core.dcache.DCacheFactory;
import org.rdlinux.luava.dcache.core.dcache.ops.COpsForValue;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DCacheTest {
    private static RedisTemplate<String, Object> redisTemplate;
    private static RedissonClient redissonClient;
    private static DCacheFactory dCacheFactory;

    static {
        GenericObjectPoolConfig<RedisConnection> pool = new GenericObjectPoolConfig<>();
        pool.setMaxIdle(5);
        pool.setMinIdle(5);
        pool.setMaxTotal(5);
        pool.setMaxWait(Duration.ofSeconds(5));
        pool.setTestOnBorrow(false);
        pool.setTimeBetweenEvictionRuns(Duration.ofSeconds(3000));

        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
        configuration.setDatabase(0);
        configuration.setHostName("192.168.1.129");
        configuration.setPort(6379);
        configuration.setPassword("123456");

        LettucePoolingClientConfiguration ltPcf = LettucePoolingClientConfiguration.builder().poolConfig(pool)
                .commandTimeout(Duration.ofSeconds(3)).build();

        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration, ltPcf);
        connectionFactory.afterPropertiesSet();

        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        om.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.NONE);
        om.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
        om.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL);
        GenericJackson2JsonRedisSerializer valueSer = new GenericJackson2JsonRedisSerializer(om);
        StringRedisSerializer keySer = new StringRedisSerializer();
        redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.setKeySerializer(keySer);
        redisTemplate.setValueSerializer(valueSer);
        redisTemplate.setHashKeySerializer(keySer);
        redisTemplate.setHashValueSerializer(valueSer);
        redisTemplate.afterPropertiesSet();
    }

    static {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.1.129:6379").setPassword("123456").setDatabase(0)
                .setConnectionMinimumIdleSize(3).setConnectionPoolSize(5);
        redissonClient = Redisson.create(config);
    }

    static {
        dCacheFactory = new DCacheFactory(redisTemplate, redissonClient);
    }

    @Test
    public void getCallTest() throws InterruptedException {
        DCache<String> cache = dCacheFactory.getCache("user", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String zhangsan = opv.get("zhangsan", key -> {
                    System.out.println("执行回调获取" + key);
                    return "李四";
                });
                System.out.println(zhangsan);
            }).start();
        }
        new CountDownLatch(1).await();
    }

    @Test
    public void multiGetCallTest() throws InterruptedException {
        DCache<String> cache = dCacheFactory.getCache("user", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Map<String, String> ret = opv.multiGet(Arrays.asList("a", "b", "c"), keys -> {
                    System.out.println("执行回调获取" + keys.toString());
                    return keys.stream().collect(Collectors.toMap(e -> e, e -> "1"));
                });
                System.out.println(ret);
            }).start();
        }
        new CountDownLatch(1).await();
    }
}
