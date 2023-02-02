package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    //起始时间戳为2023/1/1 00:00:00
    private static final long START_TIMESTAMP = 1672531200L;
    //序列号位数
    private static final int SN = 32;

    private final StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix){
        LocalDateTime now = LocalDateTime.now();
        //获取当前时间戳
        long current = now.toEpochSecond(ZoneOffset.UTC);
        //获取时间戳
        long timestamp = current - START_TIMESTAMP;
        //获取今天的日期
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));

        //Redis自增，生成今天的序列号
        String key = "inc:" + keyPrefix + ":" + date;
        Long increment = stringRedisTemplate.opsForValue().increment(key);

        //拼接时间戳和序列号
        return timestamp << SN | increment;
    }
}
