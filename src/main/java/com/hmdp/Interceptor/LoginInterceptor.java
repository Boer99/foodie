package com.hmdp.Interceptor;

import com.hmdp.dto.UserDTO;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

// 登录校验拦截器
public class LoginInterceptor implements HandlerInterceptor {

    StringRedisTemplate stringRedisTemplate;

    public LoginInterceptor(StringRedisTemplate srt) {
        stringRedisTemplate = srt;
    }

    @Override
    public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws Exception {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            res.sendError(401);
            // 拦截
            return false;
        }
        LocalDate now = LocalDate.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String key = RedisConstants.UV_KEY + keySuffix;
        // UV统计
        stringRedisTemplate.opsForHyperLogLog().add(key, String.valueOf(user.getId()));
        return true;
    }
}
