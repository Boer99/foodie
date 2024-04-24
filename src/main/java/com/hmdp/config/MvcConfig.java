package com.hmdp.config;

import com.hmdp.Interceptor.LoginInterceptor;
import com.hmdp.Interceptor.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {
    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // token刷新拦截器
        registry
                .addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate))
                .addPathPatterns("/**")
                .order(0);
        // 登录校验拦截器
        registry
                .addInterceptor(new LoginInterceptor(stringRedisTemplate))
                // 登录校验排除路径
                .excludePathPatterns(
                        "/swagger-resources/**", "/webjars/**", "/v2/**", "/swagger-ui.html/**",
                        "/api", "/api-docs", "/api-docs/**", "/doc.html/**",
                        "/shop/**",
                        "/shop-type/**",
                        "/blog/hot",
                        "/user/code",
                        "/user/login"
                )
                .order(1);
    }
}
