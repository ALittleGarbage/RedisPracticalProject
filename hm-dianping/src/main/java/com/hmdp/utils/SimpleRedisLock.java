package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    //初始化lua脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        //设置脚本位置
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("lua/unlock.lua"));
        //设置返回值类型
        UNLOCK_SCRIPT.setResultType(Long.class);
    }
    //业务名称
    private final String businessName;
    private final StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String businessName, StringRedisTemplate stringRedisTemplate) {
        this.businessName = businessName;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标识
        String value = ID_PREFIX + Thread.currentThread().getId();
        Boolean flag = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + businessName, value, timeoutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    @Override
    public void unlock() {
        //获取当前线程标示
        String value = ID_PREFIX + Thread.currentThread().getId();
        //调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + businessName),
                value);
    }

    /*@Override
    public void unlock() {
        //获取当前线程标示
        String value = ID_PREFIX + Thread.currentThread().getId();
        //获取value值
        String result = stringRedisTemplate.opsForValue().get(KEY_PREFIX + businessName);
        //判断是否为当前线程的锁
        if(value.equals(result)){
            stringRedisTemplate.delete(KEY_PREFIX + businessName);
        }
    }*/
}
