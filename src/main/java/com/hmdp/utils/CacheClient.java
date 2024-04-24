package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 将对象序列化为 json 并写入缓存，可设置 TTL
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 将对象序列化为 json 并写入缓存，可设置 TTL
     * - 可设置逻辑过期时间，处理【缓存击穿】
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 根据 key 查询缓存，并反序列化；
     * 利用“缓存空值”解决【缓存穿透】
     *
     * @param type       返回值类型
     * @param dbFallback id查询数据库逻辑，需实现dbFallback接口
     */
    public <R, ID> R queryWithPassThrough(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long time, TimeUnit unit) {

        // 查询缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            // 【缓存穿透】命中为空值""
            return null;
        }
        // 查询 db
        R r = dbFallback.apply(id);
        if (r == null) {
            // 【缓存穿透】创建无效key
            stringRedisTemplate.opsForValue()
                    .set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 写入 Redis
        this.set(key, r, time, unit);
        return r;
    }


    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long time, TimeUnit unit) {

        String key = keyPrefix + id;
        // # 查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // ## 未命中，返回空
        if (StrUtil.isBlank(json)) {
            return null;
        }
        // # 命中，判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        // 获取逻辑过期时间
        LocalDateTime expireTime = redisData.getExpireTime();
        // ## 未过期，返回r
        if (expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }
        // # 过期，获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // ## 获取到锁，开启独立线程重建缓存
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查数据库
                    R r1 = dbFallback.apply(id);
                    // 存入Redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unLock(lockKey);
                }
            });
        }
        // ## 没获取到锁，返回过期r
        return r;
    }

    /**
     * 获取锁
     *
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 不要直接返回flag，flag自动拆箱可能会空指针异常
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     *
     * @param key
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}
