package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1. 执行lua脚本
        Long res = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(), // 没有key参数
                voucherId.toString(), userId.toString()
        );
        // 2. 判断结果
        int r = res.intValue();
        // 2.1 不为0，没有购买资格
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2 为0，把下单信息保存到阻塞队列
        long orderId = redisIdWorker.nextId("order");
        // TODO 保存阻塞队列

        // 3 返回订单id
        return Result.ok(orderId);
    }

    /**
     * 秒杀下单_单机实现
     *
     * @param voucherId
     * @return
     */
    public Result singleSeckillVoucher(Long voucherId) {
        // 查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 判断秒杀是否开始或结束
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("时间未开始");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("时间已结束");
        }
        // 优惠券库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }

        // 给每个用户上锁，订单创建完，事务提交了才释放锁

        // 1、单机业务实现
//        Long userId = UserHolder.getUser().getId();
//        synchronized (userId.toString().intern()) {
//            // 获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }

        // 2、分布式锁实现
        Long userId = UserHolder.getUser().getId();
//        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        // # 获取失败
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        // # 执行业务逻辑
        try {
            // 获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 一人一单，判断当前用户是否有订单记录
        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (count > 0) {
            log.info("用户已经购买过一次");
            return Result.fail("用户已经购买过一次");
        }
        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0).update(); // CAS
        if (!success) {
            return Result.fail("库存不足");
        }
        // 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        return Result.ok(orderId);
    }

}
