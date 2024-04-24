package com.hmdp;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
@Slf4j
public class RedissionTest {
    @Resource
    private RedissonClient redissonClient;

    private RLock lock;

    @BeforeEach
    public void setUp() {
        lock = redissonClient.getLock("order");
    }

    @Test
    public void method1() {
//        boolean isLock = lock.tryLock();
//        lock = redissonClient.getLock("order");
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("获取锁失败1");
            return;
        }
        try {
            log.info("获取锁成功1");
            method2();
            log.info("开始执行业务1");
        } finally {
            log.warn("准备释放锁1");
            lock.unlock();
        }
    }

    public void method2() {
//        boolean isLock = lock.tryLock();
//        lock = redissonClient.getLock("order");
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("获取锁失败2");
            return;
        }
        try {
            log.info("获取锁成功2");
            log.info("开始执行业务2");
        } finally {
            log.warn("准备释放锁2");
            lock.unlock();
        }
    }
}
