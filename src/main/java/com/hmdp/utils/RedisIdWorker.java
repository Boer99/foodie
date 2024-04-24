package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private static final long BEGIN_TIMESTAMP = 1672531200L;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final int COUNT_BITS = 32;

    public long nextId(String keyPrefix) {
        // # 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = nowSecond - BEGIN_TIMESTAMP;
        // # 生成序列号
        // ## 获取当前日期
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // ## 序列号自增长
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        // # 拼接
        return timeStamp << COUNT_BITS | count;
    }

    public static void main(String[] args) {
        // 生成起始时间
        LocalDateTime time = LocalDateTime.of(2023, 1, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println("second=" + second);
    }
}
