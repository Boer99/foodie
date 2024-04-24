package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private String name;
    private StringRedisTemplate stringRedisTemplate;

    private static final String KEY_PREFIX = "lock:";
    private static final String THREAD_ID_PREFIX = UUID.randomUUID().toString(true) + '-';
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程标识
        String threadId = THREAD_ID_PREFIX + Thread.currentThread().getId();
        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success); // 避免空指针
    }

    @Override
    public void unLock() {
        // 调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                THREAD_ID_PREFIX + Thread.currentThread().getId()
        );
    }

//    @Override
//    public void unLock() {
//        // 获取线程标识
//        String threadId = THREAD_ID_PREFIX + Thread.currentThread().getId();
//        // 获取锁的线程标识
//        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//        // 判断标识是否一致
//        if (threadId.equals(id)) {
//            // 释放锁
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
//    }
}
