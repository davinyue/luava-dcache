package org.rdlinux.luava.dcache.test;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.Test;
import org.rdlinux.luava.dcache.CafeRedisCacheFactory;
import org.rdlinux.luava.dcache.OpvCache;
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

import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DCacheOphTest {
    private static final String redisPrefix = "test";
    private static RedisTemplate<Object, Object> redisTemplate;
    private static RedissonClient redissonClient;
    private static CafeRedisCacheFactory dCacheFactory;

    static {
        LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
        URL resource = DCacheOphTest.class.getClassLoader().getResource("log4j2-test.xml");
        try {
            assert resource != null;
            logContext.setConfigLocation(resource.toURI());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        logContext.reconfigure();
    }

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
        configuration.setHostName("192.168.163.3");
        configuration.setPort(6379);
        //configuration.setPassword("123456");

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
        config.useSingleServer().setAddress("redis://192.168.163.3:6379").setDatabase(0)
                .setConnectionMinimumIdleSize(3).setConnectionPoolSize(5);
        redissonClient = Redisson.create(config);
    }

    static {
        dCacheFactory = new CafeRedisCacheFactory(redissonClient, redisTemplate, redisPrefix);
    }

    @Test
    public void getTest() throws Exception {
        String cacheName = "users";
        String redisKey = redisPrefix + ":" + cacheName + ":";
        redisTemplate.opsForValue().set(redisKey + "user", "张三", 1, TimeUnit.HOURS);
        redisTemplate.opsForValue().set(redisKey + "2", "222", 1, TimeUnit.HOURS);
        OpvCache dCache = dCacheFactory.getWeakConsistencyOpvDCache(cacheName, 5 * 1000);
        String user = dCache.get("user");
        log.info("获取缓存user:{}", user);
        String v2 = dCache.get(2);
        log.info("获取缓存2:{}", v2);
        user = dCache.get("user");
        log.info("二次获取缓存user:{}", user);
        TimeUnit.SECONDS.sleep(10);
        user = dCache.get("user");
        log.info("三次次获取缓存user:{}", user);
    }

    @Test
    public void getCallTest() throws Exception {
        String cacheName = "users";
        OpvCache dCache = dCacheFactory.getWeakConsistencyOpvDCache(cacheName, 5 * 1000);
        int age = dCache.get("age", key -> 2);
        log.info("年龄:{}", age);
        log.info("二次获取");
        age = dCache.get("age", key -> 2);
        log.info("年龄:{}", age);
    }

    @Test
    public void multiGetTest() throws Exception {
        String cacheName = "users";
        OpvCache dCache = dCacheFactory.getWeakConsistencyOpvDCache(cacheName, 5 * 1000);
        Set<Object> keys = new HashSet<>();
        keys.add("user");
        keys.add(2);
        Map<Object, Object> kvs = dCache.multiGet(keys);
        log.info("数据:{}", kvs);
    }

    @Test
    public void multiGetCallTest() throws Exception {
        String cacheName = "users";
        OpvCache dCache = dCacheFactory.getWeakConsistencyOpvDCache(cacheName, 5 * 1000);
        Set<String> keys = new HashSet<>();
        keys.add("李四age");
        keys.add("张三age");
        Map<String, Integer> ages = dCache.multiGet(keys, callKeys -> {
            Map<String, Integer> ret = new HashMap<>();
            ret.put("李四age", 23);
            ret.put("张三age", 23);
            return ret;
        });
        log.info("年龄:{}", ages);
        log.info("二次获取");
        ages = dCache.multiGet(keys, callKeys -> {
            Map<String, Integer> ret = new HashMap<>();
            ret.put("李四age", 23);
            ret.put("张三age", 23);
            return ret;
        });
        log.info("年龄:{}", ages);
    }

    @Test
    public void deleteTest() throws Exception {
        String cacheName = "users";
        OpvCache dCache = dCacheFactory.getWeakConsistencyOpvDCache(cacheName, 5 * 1000);
        dCache.set("lisi", "lisi");
        dCache.set("2", 2);
        log.info("lisi:{}", (String) dCache.get("lisi"));
        log.info("2:{}", (int) dCache.get(2));
        dCache.delete("lisi");
        dCache.delete(2);
        log.info("删除后获取");
        log.info("lisi:{}", (String) dCache.get("lisi"));
        log.info("2:{}", (Integer) dCache.get(2));
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(10, TimeUnit.SECONDS);
    }

    @Test
    public void mThreadGetTest() throws Exception {
        String cacheName = "users";
        OpvCache dCache = dCacheFactory.getWeakConsistencyOpvDCache(cacheName, 5 * 1000);
        int count = 20;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                try {
                    countDownLatch.countDown();
                    countDownLatch.await();
                    String value = dCache.get("lisi", k -> "sili");
                    log.info("value:{}", value);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        new CountDownLatch(1).await(10, TimeUnit.SECONDS);
    }

    @Test
    public void mClientDeleteTest() throws Exception {
        String cacheName = "users";
        int count = 2;
        OpvCache[] dCaches = new OpvCache[count];
        dCaches[0] = dCacheFactory.getWeakConsistencyOpvDCache(cacheName, 5 * 1000);
        dCaches[1] = dCacheFactory.getWeakConsistencyOpvDCache(cacheName, 5 * 1000);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            int h = i;
            new Thread(() -> {
                try {
                    countDownLatch.countDown();
                    countDownLatch.await();
                    dCaches[h].delete("list");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        new CountDownLatch(1).await(10, TimeUnit.SECONDS);
    }
}
