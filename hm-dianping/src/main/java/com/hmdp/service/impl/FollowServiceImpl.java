package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
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

    @Override
    public Result isFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        //先从Redis中查找是否已经关注了
        Boolean isMember = stringRedisTemplate.opsForSet()
                .isMember(RedisConstants.FOLLOW_KEY + userId, followUserId.toString());
        if (BooleanUtil.isTrue(isMember)){
            return Result.ok(true);
        }

        //select count(*) from tb_follow where user_id = ? and follow_user_id = ?
        Integer count = query().eq("follow_user_id", followUserId)
                .eq("user_id", userId).count();

        //判断是否大于0
        if (count > 0) {
            //从Redis添加关注
            stringRedisTemplate.opsForSet()
                    .add(RedisConstants.FOLLOW_KEY + userId, followUserId.toString());
        }

        return Result.ok(count > 0);
    }

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();

        if (BooleanUtil.isTrue(isFollow)) {
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            //保存
            boolean isSuccess = save(follow);
            if (isSuccess) {
                //从Redis添加关注
                stringRedisTemplate.opsForSet()
                        .add(RedisConstants.FOLLOW_KEY + userId, followUserId.toString());
            }
        }
        else {
            //delete from tb_follow where userId = ? and follow_user_id = ?
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("follow_user_id", followUserId)
                    .eq("user_id", userId));
            if (isSuccess) {
                //从Redis中移除关注
                stringRedisTemplate.opsForSet()
                        .remove(RedisConstants.FOLLOW_KEY + userId, followUserId.toString());
            }
        }

        return Result.ok();
    }

    @Override
    public Result commonFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();

        Set<String> common = stringRedisTemplate.opsForSet().intersect(
                RedisConstants.FOLLOW_KEY + userId,
                RedisConstants.FOLLOW_KEY + followUserId);

        //判断是否为null或者为空，则返回空列表
        if (common == null || common.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        //解析userId
        List<Long> userIds = common
                .stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());

        //获取userDto信息
        List<UserDTO> commonList = userService.listByIds(userIds)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(commonList);
    }
}
