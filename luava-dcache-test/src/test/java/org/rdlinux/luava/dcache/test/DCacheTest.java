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

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
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
        int tn = 20;
        CountDownLatch latch = new CountDownLatch(tn);
        for (int i = 0; i < tn; i++) {
            new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String zhangsan = opv.get("zhangsan", key -> {
                    log.info("执行回调获取" + key);
                    return "李四";
                });
                log.info("缓存结果:{}", zhangsan);
            }).start();
        }
        new CountDownLatch(1).await();
    }

    @Test
    public void multiGetCallTest() throws InterruptedException {
        DCache<String> cache = dCacheFactory.getCache("user", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        int tn = 20;
        CountDownLatch latch = new CountDownLatch(tn);
        for (int i = 0; i < tn; i++) {
            new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Map<String, String> ret = opv.multiGet(Arrays.asList("a", "b", "c"), keys -> {
                    log.info("执行回调获取" + keys.toString());
                    return keys.stream().collect(Collectors.toMap(e -> e, e -> "1"));
                });
                log.info("缓存结果:{}", ret.toString());
            }).start();
        }
        new CountDownLatch(1).await();
    }

    /**
     * 单线程设置测试
     */
    @Test
    public void singleThreadSetTest() {
        DCache<String> cache = dCacheFactory.getCache("user", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        long start = System.currentTimeMillis();
        int total = 10000;
        for (int i = 0; i < total; i++) {
            String key = UUID.randomUUID().toString().replaceAll("-", "").substring(8, 24);
            opv.set(key, key);
        }
        long end = System.currentTimeMillis();
        log.info("设置{}条数据,单线程耗时:{}毫秒", total, end - start);
    }

    /**
     * 多线程设置测试
     */
    @Test
    public void multiThreadSetTest() throws Exception {
        DCache<String> cache = dCacheFactory.getCache("user", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        int total = 10000;
        int threadN = 5;
        int part = total / threadN;
        CountDownLatch latch = new CountDownLatch(threadN);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadN; i++) {
            new Thread(() -> {
                for (int h = 0; h < part; h++) {
                    String key = UUID.randomUUID().toString().replaceAll("-", "").substring(8, 24);
                    opv.set(key, key);
                }
                latch.countDown();
            }).start();
        }
        latch.await();
        long end = System.currentTimeMillis();
        log.info("设置{}条数据,多线程耗时:{}毫秒", total, end - start);
    }

    /**
     * 单线程设置测试
     */
    @Test
    public void singleThreadGetTest() {
        DCache<String> cache = dCacheFactory.getCache("user", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        opv.set("a", "1");
        long start = System.currentTimeMillis();
        int total = 10000;
        for (int i = 0; i < total; i++) {
            opv.get("a");
        }
        long end = System.currentTimeMillis();
        log.info("获取{}条数据,单线程耗时:{}毫秒", total, end - start);
    }

    /**
     * 多线程设置测试
     */
    @Test
    public void multiThreadGetTest() throws Exception {
        DCache<String> cache = dCacheFactory.getCache("user", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        opv.set("a", "1");
        int total = 10000;
        int threadN = 5;
        int part = total / threadN;
        CountDownLatch latch = new CountDownLatch(threadN);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadN; i++) {
            new Thread(() -> {
                for (int h = 0; h < part; h++) {
                    opv.get("a");
                }
                latch.countDown();
            }).start();
        }
        latch.await();
        long end = System.currentTimeMillis();
        log.info("获取{}条数据,多线程耗时:{}毫秒", total, end - start);
    }

    /**
     * 生成一万个key到缓存, 并将key存储到文件
     */
    @Test
    public void generateKeysTests() throws Exception {
        DCache<String> cache = dCacheFactory.getCache("order", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        long start = System.currentTimeMillis();
        int total = 10000;
        File file = new File("d:/dCacheKeys.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter writer = new FileWriter("d:/dCacheKeys.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        for (int i = 0; i < total; i++) {
            String key = UUID.randomUUID().toString().replaceAll("-", "").substring(8, 24);
            opv.set(key, key);
            bufferedWriter.write(key + "\r\n");
        }
        long end = System.currentTimeMillis();
        bufferedWriter.close();
        writer.close();
        log.info("设置{}条数据,单线程耗时:{}毫秒", total, end - start);
    }

    /**
     * 单线程从文件读取key
     */
    @Test
    public void getKeysFromFileTests() throws Exception {
        DCache<String> cache = dCacheFactory.getCache("order", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        opv.set("a", "1");
        long start = System.currentTimeMillis();
        int total = 200;
        FileReader reader = new FileReader("d:/dCacheKeys.txt");
        BufferedReader bufferedReader = new BufferedReader(reader);
        for (int i = 0; i < total; i++) {
            String key = bufferedReader.readLine();
            opv.get(key);
        }
        long end = System.currentTimeMillis();
        bufferedReader.close();
        reader.close();
        log.info("单线程获取{}条数据,单线程耗时:{}毫秒", total, end - start);
        new CountDownLatch(1).await();
    }

    /**
     * 多线程从文件读取key
     */
    @Test
    public void mtGetKeysFromFileTests() throws Exception {
        DCache<String> cache = dCacheFactory.getCache("order", 1, TimeUnit.DAYS);
        COpsForValue<String> opv = cache.opsForValue();
        opv.set("a", "1");
        long start = System.currentTimeMillis();
        int total = 2000;
        int threadNum = 5;
        int threadDataNum = total / threadNum;
        CountDownLatch latch = new CountDownLatch(threadNum);
        FileReader reader = new FileReader("d:/dCacheKeys.txt");
        BufferedReader bufferedReader = new BufferedReader(reader);
        List<String[]> thData = new LinkedList<>();
        for (int i = 0; i < threadNum; i++) {
            String[] data = new String[threadDataNum];
            for (int h = 0; h < threadDataNum; h++) {
                data[h] = bufferedReader.readLine();
            }
            thData.add(data);
        }
        for (int i = 0; i < threadNum; i++) {
            int finalI = i;
            new Thread(() -> {
                String[] data = thData.get(finalI);
                for (int h = 0; h < threadDataNum; h++) {
                    opv.get(data[h]);
                }
                latch.countDown();
            }).start();
        }
        latch.await();
        long end = System.currentTimeMillis();
        bufferedReader.close();
        reader.close();
        log.info("多线程获取{}条数据,单线程耗时:{}毫秒", total, end - start);
    }
}
