package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //获取token
        String token = request.getHeader("authorization");
        //token不存在，直接放行，不添加到ThreadLocal
        if (StrUtil.isBlank(token)) {
            return true;
        }
        //获取用户信息
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(RedisConstants.LOGIN_USER_KEY + token);
        //用户信息不存在，直接放行，不添加到ThreadLocal
        if (userMap.isEmpty()){
            return true;
        }
        //将map转换成UserDto
        UserDTO user = BeanUtil.fillBeanWithMap(userMap,new UserDTO(),false);
        //保存到TheadLocal
        UserHolder.saveUser(user);
        //刷新有效期
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY + token, //key
                RedisConstants.LOGIN_USER_TTL, // 30
                TimeUnit.MINUTES); //分钟
        //放行
        return true;
    }

    //整个请求完成执行，主要作用是用于清理资源
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        //移除用户
        UserHolder.removeUser();
    }
}
