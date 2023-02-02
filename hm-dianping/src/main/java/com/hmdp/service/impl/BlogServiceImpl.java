package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScoreResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
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

/**
 * <p>
 *  服务实现类
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
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            //设置名字头像
            queryUserToBlog(blog);
            //设置当前blog是否已经点赞
            isLikedBlog(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);

        if (blog == null){
            return Result.fail("笔记不存在");
        }
        //设置名字、头像
        queryUserToBlog(blog);
        //设置查询的blog是否已经点赞
        isLikedBlog(blog);

        return Result.ok(blog);
    }

    @Override
    public Result likeBlog(Long id) {
        //不需要判断，因为有登录拦截器拦截路径
        Long userId = UserHolder.getUser().getId();
        //从Redis中查询是否点赞过
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //如果未点赞，则点赞
        if (score == null){
            //更新数据库中blog的点赞量
            boolean isSuccess = update()
                    .setSql("liked = liked + 1")
                    .eq("id", id).update();
            //如果更新成功
            if (isSuccess){
                //将userId放入到Redis的sortedSet集合中,以时间戳为score
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }
        //如果已经点赞，则取消点赞
        else {
            //更新数据库中blog的点赞量
            boolean isSuccess = update()
                    .setSql("liked = liked - 1")
                    .eq("id", id).update();
            //如果更新成功
            if (isSuccess){
                //从Redis的set集合中移除userId
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }

        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        //查询点赞前5的userId
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0L, 4L);
        //判断是否为null或者为空，则返回空列表
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        //解析userId
        List<Long> userIds = top5
                .stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        String join = StrUtil.join(",", userIds);

        //获取userDto信息，获取的信息应该也要按照先后顺序
        //WHERE id IN 5,1 ORDER BY FIELD(id,5,1)
        List<UserDTO> top5List = userService.query()
                .in("id", userIds)
                .last("ORDER BY FIELD(id," + join + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(top5List);
    }

    @Override
    public Result saveBlog(Blog blog) {
        Long userId = UserHolder.getUser().getId();
        blog.setUserId(userId);
        //将blog保存到数据库
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("新增图书失败");
        }
        //查询到所有粉丝
        List<Follow> followList = followService.query().eq("follow_user_id", userId).list();
        //遍历将blog的id放入到粉丝的Redis的sortedset集合中
        for (Follow follow : followList) {
            Long id = follow.getUserId();
            stringRedisTemplate.opsForZSet()
                    .add(RedisConstants.FEED_KEY + id,
                            blog.getId().toString(),
                            System.currentTimeMillis());
        }

        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Integer offset, Long maxTime) {
        Long userId = UserHolder.getUser().getId();
        //获取滚动分页数据
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(
                        RedisConstants.FEED_KEY + userId,
                        0, maxTime,
                        offset, 3);

        //判断是否为null或者为空，则返回空列表
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        //解析出blogId列表、minTime最小时间戳、offset偏移量
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        offset = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            String blogIdStr = typedTuple.getValue();
            blogIds.add(Long.valueOf(blogIdStr));
            long time = typedTuple.getScore().longValue();
            if (time == minTime){
                offset++;
            }
            else{
                minTime = time;
                offset = 1;
            }
        }

        String join = StrUtil.join(",", blogIds);
        //根据blogId，获得按照顺序的blogList列表
        List<Blog> blogList = query()
                .in("id", blogIds)
                .last("ORDER BY FIELD(id," + join + ")").list();

        //添加是否已点赞，以及名字、头像
        for (Blog blog : blogList) {
            queryUserToBlog(blog);
            isLikedBlog(blog);
        }

        //封装数据，返回
        ScoreResult scoreResult = new ScoreResult();
        scoreResult.setList(blogList);
        scoreResult.setOffset(offset);
        scoreResult.setMinTime(minTime);

        return Result.ok(scoreResult);

    }

    private void isLikedBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();
        //需要判断，没有登录拦截器拦截路径
        if (user == null){
            return;
        }
        Long userId = user.getId();

        //从Redis中查询是否点赞过
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }


    private void queryUserToBlog(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
