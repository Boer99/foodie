package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Resource
    private IFollowService followService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
//        records.forEach(blog -> queryBlogUser(blog));
//        records.forEach(this::queryBlogUser);
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        // db查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在");
        }
        // 设置blog用户信息
        queryBlogUser(blog);
        // 判断当前用户是否点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    /**
     * blog是否被当前用户点赞
     */
    private void isBlogLiked(Blog blog) {
        // 获取登录用户
        UserDTO userDTO = UserHolder.getUser();
        if (userDTO == null) {
            return;
        }
        Long userId = userDTO.getId();
        // ZSET 判断当前用户是否点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    /**
     * 查询笔记的用户信息
     *
     * @param blog
     */
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    /**
     * 当前用户 点赞/取消点赞
     */
    @Override
    public Result likeBlog(Long blogId) {
        Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY + blogId;
        // zscore k，查询 ZSET 里是否存在 userId
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            // 没点赞点赞
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", blogId).update();
            if (isSuccess) {
                // zadd k v score，添加 userId 到 ZSET 中，score为时间戳
                stringRedisTemplate.opsForZSet()
                        .add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 点赞了取消
            update().setSql("liked = liked - 1").eq("id", blogId).update();
            // zset 集合移除 userId
            stringRedisTemplate.opsForZSet().remove(key, userId.toString());
        }
        return Result.ok();
    }

    /**
     * 点赞排行榜：获取最先点赞的top5用户
     */
    @Override
    public Result queryBlogLikes(Long blogId) {
        String key = BLOG_LIKED_KEY + blogId;
        // ZRANGE k start end，获取top5
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // userId，String-->Long
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String strIds = StrUtil.join(",", ids);
        // db 查询用户信息
        List<UserDTO> userDTOS = userService
                // 注意：mysql的in不保证有序性，order by field(id, id1, id2 ...) 保证有序
                .query().in("id", ids).last("ORDER BY FIELD(id," + strIds + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(userDTOS);
    }

    /**
     * 发blog，推送到粉丝的收件箱
     * @param blog
     * @return
     */
    @Override
    public Result saveBlog(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2.保存探店博文
        boolean isSuccess = save(blog);
        if (BooleanUtil.isFalse(isSuccess)) {
            return Result.fail("新增blog失败");
        }
        // 3.查询blog作者的所有粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", blog.getUserId()).list();
        // 4.推送blog到粉丝的收件箱
        for (Follow follow : follows) {
            // 4.1 获取粉丝id
            Long userId = follow.getUserId();
            // 4.2 推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 5.返回blogId
        return Result.ok(blog.getId());
    }

    /**
     * 查询关注列表里，关注用户的最新博文，下拉（滚动）刷新
     *
     * @param max    最大的时间戳
     * @param offset 偏移量，从第几条开始查
     * @return
     */
    @Override
    public Result queryBlogOfFollow(long max, Integer offset) {
        // 1 获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2 查询收件箱
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 3 非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 4 解析收件箱数据
        // zset={5,5,5,5,3,3,2}
        // 第一次：max=6, offset=0, res={5,5}, offset=2,
        // 第二次：
        long minTime = 0;
        int os = 1;
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 4.2 获取分数（时间戳）
            long time = typedTuple.getScore().longValue();
            // 4.1 获取blogId
            blogIds.add(Long.valueOf(typedTuple.getValue()));
            // 获取最小时间戳、偏移量
            if (time == minTime) {
                os++;
            } else {
                minTime = time;
                os = 1;
            }
        }
        // 5 根据id查询blog
        String strIds = StrUtil.join(",", blogIds);
        List<Blog> blogs = query().in("id", blogIds).last("ORDER BY FIELD(id," + strIds + ")").list();
        // 6 查询笔记的用户信息，被点赞信息
        for (Blog blog : blogs) {
            queryBlogUser(blog);
            isBlogLiked(blog);
        }
        // 7 封装返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);
        return Result.ok(r);
    }

}
