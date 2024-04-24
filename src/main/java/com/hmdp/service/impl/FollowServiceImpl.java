package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;

    /**
     * 关注和取关
     *
     * @param followUserId
     * @param isFollow     是否关注了
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 获取登录用户
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        // 2 关注
        if (isFollow) {
            // 2.1 新增关注数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean success = save(follow);
            // 2.2 将关注用户的id存入Redis的Set集合
            if (success) {
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            // 3 取关
            // 3.1 删除关注数据
            boolean success = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId)
                    .eq("follow_user_id", followUserId));
            // 3.2 将关注用户的id从Redis的Set集合移除
            if (success) {
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    /**
     * 判断是否关注了该用户
     *
     * @param followUserId
     * @return
     */
    @Override
    public Result isFollow(Long followUserId) {
        // 1 获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 2 查询是否关注
        Integer count = query()
                .eq("user_id", userId)
                .eq("follow_user_id", followUserId)
                .count();
        // 3 返回
        return Result.ok(count > 0);
    }

    /**
     * 查找共同关注
     *
     * @param userId2
     * @return
     */
    @Override
    public Result followCommon(Long userId2) {
        // 1 获取当前user
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        String key2 = "follows:" + userId2;
        // 2 求关注交集
        Set<String> intersectIds = stringRedisTemplate.opsForSet().intersect(key, key2);
        // 2.1 无交集
        if (intersectIds == null || intersectIds.isEmpty()) {
            // 无交集
            return Result.ok(Collections.emptyList());
        }
        // 3 有交集，解析id交集
        List<Long> ids = intersectIds.stream().map(Long::valueOf).collect(Collectors.toList());
        // 4 查询用户并封装
        List<UserDTO> userDTOS = userService.listByIds(ids).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 5 返回
        return Result.ok(userDTOS);
    }
}
