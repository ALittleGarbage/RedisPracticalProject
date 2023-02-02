package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit timeUnit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    public void setWithExpire(String key, Object value, Long time, TimeUnit timeUnit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        //写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透
    public <T, I> T queryWithPenetration(String keyPrefix,
                        I id, Class<T> type,
                        Function<I, T> dbFallback,
                        Long time, TimeUnit timeUnit){
        String key = keyPrefix + id;

        String json = stringRedisTemplate.opsForValue().get(key);
        //不为null和""
        if (StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        //为""
        if ("".equals(json)){
            return null;
        }
        //数据库查询
        T t = dbFallback.apply(id);
        //不存在，写入空值
        if (t == null){
            this.set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //存在
        this.set(key, t, time, timeUnit);

        return t;
    }

    //缓存击穿基于互斥锁
    public <T, I> T queryWithBreakdownByMutex(String keyPrefix,
                                              I id, Class<T> type,
                                              Function<I, T> dbFallback,
                                              Long time, TimeUnit timeUnit){
        String key = keyPrefix + id;
        //从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在，不为null和空字符串""的情况
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        //命中的是否为空字符串
        if ("".equals(json)) {
            return null;
        }

        T t = null;
        String lock = RedisConstants.LOCK_SHOP_KEY + id;
        try {
            //尝试获取互斥锁
            boolean isLock = tryLock(lock);
            //获取失败
            if (!isLock){
                Thread.sleep(50);
                //递归，直到能够获取到shop
                return queryWithBreakdownByMutex(keyPrefix, id, type, dbFallback, time, timeUnit);
            }
            //为null不存在，根据id查询
            t = dbFallback.apply(id);
            //id不存在，返回错误信息
            if (t == null) {
                //将空值写入Redis
                this.set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //id存在，将数据写入Redis，设置有效期
            this.set(key, JSONUtil.toJsonStr(t), time, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁，finally的执行早于try里面的return
            unlock(lock);
        }

        return t;
    }

    //缓存击穿利用逻辑过期
    public <T, I> T queryWithBreakdownByExpireTime(String keyPrefix,
                                              I id, Class<T> type,
                                              Function<I, T> dbFallback,
                                              Long addTime, TimeUnit timeUnit){
        String key = keyPrefix + id;
        //从Redis查询商铺缓存
        String redisDataShopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在，为null和空字符串""的情况
        if (StrUtil.isBlank(redisDataShopJson)) {
            //不存在，就不是热点key，一般热点商铺都会提前存入Redis，返回错误信息
            return null;
        }
        //json转对象
        RedisData redisData = JSONUtil.toBean(redisDataShopJson, RedisData.class);
        T t = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        //判断是否过期
        if(!LocalDateTime.now().isAfter(redisData.getExpireTime())){
            //未过期，返回
            return t;
        }

        String lock = RedisConstants.LOCK_SHOP_KEY + id;
        //获取互斥锁成功
        if(tryLock(lock)){
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //注意！需要进行双重检查，因为线程b在线程a重建缓存前，判断shop过期
                    //在线程a释放锁后，因为某种原因线程b才开始获取互斥锁，导致重复重建缓存

                    //重建缓存
                    this.setWithExpire(key, dbFallback.apply(id), addTime, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lock);
                }
            });
        }
        //无论是否过期都返回
        return t;
    }

    private boolean tryLock(String lockKey){
        //尝试获取互斥锁，设置有效时间
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(lockKey,
                "1",
                10, //设置10秒
                TimeUnit.SECONDS);
        //判断是否获取锁
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String lockKey){
        //解锁，删除key
        stringRedisTemplate.delete(lockKey);
    }
}
