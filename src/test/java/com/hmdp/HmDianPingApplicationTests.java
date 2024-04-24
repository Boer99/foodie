package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;

import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
@Slf4j
public class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    public void testRedisIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("testOrder");
                System.out.println("id=" + id);
            }
            // 减少计数器
            latch.countDown();
        };

        long begin = System.currentTimeMillis();
        // 共生成30k个全局id
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        // 挂起线程，计数器为0的时候恢复线程
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time=" + (end - begin));
    }

    @Test
    public void testSaveShopToRedis() {
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.SECONDS);
    }

    /**
     * 将店铺地理信息存入Redis
     */
    @Test
    public void loadShopData() {
        // 1 查询店铺信息
        List<Shop> shopList = shopService.list();
        // 2 店铺按照typeId分组
        Map<Long, List<Shop>> shopMap = shopList.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 3 分批写入Redis，同一类型商铺一批
        for (Map.Entry<Long, List<Shop>> entry : shopMap.entrySet()) {
            // 获取商铺类型id
            Long typeId = entry.getKey();
            // 获取同一类型的全部商铺
            List<Shop> shops = entry.getValue();
            // 将商铺的地理坐标存起来
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
            for (Shop shop : shops) {
                locations.add(new RedisGeoCommands.GeoLocation<String>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            String key = "shop:geo:" + typeId;
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

    @Test
    public void testHyperLogLog() {
        String[] users = new String[1000];
        // 数组角标
        int index = 0;
        for (int i = 1; i <= 1000000; i++) {
            users[index++] = "user_" + i;
            if (i % 1000 == 0) {
                index = 0;
                stringRedisTemplate.opsForHyperLogLog().add("hll1", users);
            }
        }
        Long size = stringRedisTemplate.opsForHyperLogLog().size("hll1");
        System.out.println("size=" + size);
    }

    @Test
    public void testLocalDate(){
        LocalDate now = LocalDate.now();
        String day = now.format(DateTimeFormatter.ofPattern(":yyyyMMdd"));
        log.debug(day);
    }
}
