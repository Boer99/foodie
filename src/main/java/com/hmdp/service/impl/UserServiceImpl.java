package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

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
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone) {
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }
        String code = RandomUtil.randomNumbers(6);
        // 过期时间为2分钟
        stringRedisTemplate.opsForValue()
                .set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // 模拟发送验证码给用户
        log.info("发送短信验证码成功，验证码：{}", code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm) {
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }
        // 校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        // ！！！这里不能用 cacheCode != loginForm.getCode()
        if (cacheCode == null || !cacheCode.equals(loginForm.getCode())) {
            return Result.fail("验证码错误！");
        }
        // 获取用户（不存在就创建）
        User user = query().eq("phone", phone).one();
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        // 保存用户信息到Redis，key=token, value=userMap
        // 1、key
        String token = UUID.randomUUID().toString();
        String tokenKey = LOGIN_USER_KEY + token;
        // 2、value
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // userDTO的id是Long类型，String序列化会报错
        // BeanUtil是hutool工具包
        Map<String, Object> userMap = BeanUtil.beanToMap(
                userDTO,
                new HashMap<>(),
                CopyOptions.create()
                        // 是否忽略空字段
                        .setIgnoreNullValue(true)
                        // 自定义属性值转换规则
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString())
        );
        // 3、保存
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);
        return Result.ok(token);
    }

    /**
     * 用户签到
     *
     * @return
     */
    @Override
    public Result sign() {
        // 1 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3 拼接key：前缀+userId+年月
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4 获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5 写入Redis：SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    /**
     * 统计签到天数
     */
    @Override
    public Result signCount() {
        // 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        // key：前缀+userId+年月
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        int dayOfMonth = now.getDayOfMonth();
        // 获取本月截止到今天的所有签到记录，返回的是一个10进制数字
        // BITFIELD sign:5:202203 GET u14
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create() // 这里采用批量执行命令的方式
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)) // 无符号、截止到dayOfMonth
                        .valueAt(0) // 从角标0开始
        );
        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }
        Long num = result.get(0);
        if (num == null || num == 0) {
            // 十进制0代表没签到
            return Result.ok(0);
        }
        // 6 获取连续签到天数
        int count = 0;
        while (true) {
            // 和1做与运算，得到数字最后一个bit位
            if ((num & 1) == 0) {
                // 最后一位为0，改天未签到
                break;
            } else {
                count++;
            }
            // 右移，到前一天
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        // 电话、nickname
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }

}
