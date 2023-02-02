package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }
        //符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        //保存验证码到Redis，设置有效期
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY +phone, //key
                code, //value
                RedisConstants.LOGIN_CODE_TTL, // 2
                TimeUnit.MINUTES);//指定单位为分钟
        //发送验证码，模拟一下
        log.debug("发送验证码成功，验证码为："+code);
        //返回OK
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //校验手机号
        if (RegexUtils.isPhoneInvalid(loginForm.getPhone())) {
            //不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }
        //校验验证码
        String code = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + loginForm.getPhone());
        if (code == null || !code.equals(loginForm.getCode())) {
            //不一致，返回错误信息
            return Result.fail("验证码错误");
        }
        //一致，查询手机号是否存在
        User user = query().eq("phone", loginForm.getPhone()).one();
        if (user == null) {
            //不存在，插入到数据库，完成注册
            user = createUserByPhone(loginForm.getPhone());
        }
        //随机生成登录令牌token，
        String token = UUID.randomUUID().toString(true);
        //将User对象转成UserDto在转换成Map数据类型
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //map数据类型的键值对都需要是String类型的
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,
                new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true) //忽略空的值
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));//所有value值转换为String
        //保存用户到Redis
        String key = RedisConstants.LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(key, userMap);
        //设置有效期30分钟
        stringRedisTemplate.expire(key, //key
                RedisConstants.LOGIN_USER_TTL, // 30
                TimeUnit.MINUTES); //分钟
        //返回token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        //获取年月
        String format = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = RedisConstants.USER_SIGN_KEY + userId + format;
        //获取本月的第几天
        int day = now.getDayOfMonth();
        //判断今天是否已经签到过
        Boolean isSuccess = stringRedisTemplate.opsForValue().getBit(key, day - 1);
        if (BooleanUtil.isTrue(isSuccess)){
            return Result.fail("你今天已经签到过了");
        }
        //签到
        stringRedisTemplate.opsForValue().setBit(key, day-1, true);

        return Result.ok();
    }

    @Override
    public Result signCount() {
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        //获取年月
        String format = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = RedisConstants.USER_SIGN_KEY + userId + format;
        //获取本月的第几天
        int day = now.getDayOfMonth();
        //获取本月截至今天为止的所有的签到记录，返回的是一个十进制的数字
        //BITFIELD sign:userId:yyyyMM GET day 0
        List<Long> values = stringRedisTemplate.opsForValue()
                .bitField(
                        key,
                        BitFieldSubCommands.create()
                                .get(BitFieldSubCommands.BitFieldType.unsigned(day))
                                .valueAt(0));
        if (values == null || values.isEmpty()) {
            return Result.ok(0);
        }
        //获取bit
        Long num = values.get(0);
        if (num == null) {
            return Result.ok(0);
        }

        int count = 0;
        //从后往前，如果为最后一位为0，则退出循环
        while ((num & 1) != 0) {
            //不为0，说明已签到
            count++;
            //num无符号右移1位
            num >>>= 1;
        }

        return Result.ok(count);
    }

    private User createUserByPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomNumbers(10));
        save(user);
        return user;
    }
}
