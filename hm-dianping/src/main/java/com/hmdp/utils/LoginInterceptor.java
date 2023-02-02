package com.hmdp.utils;

import org.springframework.web.servlet.HandlerInterceptor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //判断ThreadLocal是否存在用户
        if (UserHolder.getUser() == null){
            //没有，拦截，设置状态码
            response.setStatus(401);
            return false;
        }
        //有用户放行
        return true;
    }
}
