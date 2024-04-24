package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    // 线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Override
    public Result queryShopById(Long id) {
        // 解决缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(
                CACHE_SHOP_KEY,
                id,
                Shop.class,
                // shopId -> getById(shopId),
                this::getById,
                CACHE_SHOP_TTL,
                TimeUnit.MINUTES
        );

        // 互斥锁解决缓存击穿
        // Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
//        Shop shop = cacheClient.queryWithLogicalExpire(
//                CACHE_SHOP_KEY,
//                id,
//                Shop.class,
//                // shopId -> getById(shopId),
//                this::getById,
//                CACHE_SHOP_TTL,
//                TimeUnit.MINUTES
//        );

        if (shop == null) {
            return Result.fail("商铺不存在");
        }
        return Result.ok(shop);
    }

    // 重建缓存
    public void saveShopToRedis(Long id, Long expireSeconds) throws InterruptedException {
        // db 查询
        Shop shop = getById(id);
        Thread.sleep(200); // 模拟延时
        // 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 写入Redis，不设置TTL
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    /**
     * id查询商铺——缓存击穿解决——互斥锁
     */
    public Shop queryWithMutex(Long id) {
        // 查询缓存
        String shopKey = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        if (StrUtil.isNotBlank(shopJson)) {
            // 命中直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if (shopJson != null) {
            // 【缓存穿透】 命中为空值""
            return null;
        }
        // 未命中，查询 db
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            // 获取【互斥锁】
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                // 获取失败，等待缓存数据重建，递归调用
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            shop = getById(id);
            Thread.sleep(200); // 模拟重建的延迟
            if (shop == null) {
                // 【缓存穿透】 创建无效key
                stringRedisTemplate.opsForValue()
                        .set(shopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 写入缓存，【超时剔除】
            stringRedisTemplate.opsForValue()
                    .set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放【互斥锁】
            unLock(lockKey);
        }
        return shop;
    }


    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long shopId = shop.getId();
        if (shopId == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1 更新数据库
        updateById(shop);
        // 2 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
        return Result.ok();
    }

    /**
     * 根据商铺类型分页查询商铺信息
     *
     * @param typeId  商铺类型
     * @param current 页码
     * @param x
     * @param y
     * @return 商铺列表
     */
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if (x == null || y == null) {
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 查询Redis，按照（坐标点）距离排序、分页
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> geoResults = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 解析出id和distance
        if (geoResults == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> geoList = geoResults.getContent();
        if (geoList.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        // 4.1 截取from到end的部分
        List<Long> shopIds = new ArrayList<>(geoList.size());
        Map<String, Distance> distanceMap = new HashMap<>(geoList.size());
        geoList.stream()
                .skip(from)
                .forEach(result -> {
                    // 获取商铺id
                    String shopId = result.getContent().getName();
                    shopIds.add(Long.valueOf(shopId));
                    // 获取商铺距离
                    Distance distance = result.getDistance();
                    distanceMap.put(shopId, distance);
                });
        // 5 根据id查询shops
        String strIds = StrUtil.join(",", shopIds);
        List<Shop> shops = query().in("id", shopIds).last("ORDER BY FIELD(id," + strIds + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6 返回
        return Result.ok(shops);
    }


    /**
     * 获取锁
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 直接返回flag，自动拆箱可能会空指针异常
        return BooleanUtil.isTrue(flag);
    }


    /**
     * 释放锁
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }


    /**
     * id查询商铺——缓存击穿解决——互斥锁+逻辑过期
     */
    public Shop queryWithLogicalExpire(Long id) {
        // 查询缓存
        String shopKey = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        if (StrUtil.isBlank(shopJson)) {
            // 未命中
            return null;
        }
        // 判断缓存是否逻辑过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期
            return shop;
        }
        // 过期，获取【互斥锁】
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        if (isLock) {
            // 开启独立线程重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShopToRedis(id, 20L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放【互斥锁】
                    unLock(LOCK_SHOP_KEY + id);
                }
            });
        }
        // 抢锁失败，返回过期shop
        return shop;
    }

    /**
     * id查询商铺——缓存穿透解决
     */
    public Shop queryWithPassThrough(Long id) {
        // 1 查询缓存
        String shopKey = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        // 2 命中直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // ===== 缓存穿透，命中为空值""，返回错误
        if (shopJson != null) {
            return null;
        }
        // 3 未命中查数据库
        Shop shop = getById(id);
        // 3.1 不存在，返回404
        if (shop == null) {
            // ===== 缓存穿透，将空值""写入Redis
            stringRedisTemplate.opsForValue()
                    .set(shopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 3.2 存在商铺，写入Redis，返回商铺信息
        // ===== 超时剔除策略兜底
        stringRedisTemplate.opsForValue()
                .set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

}
