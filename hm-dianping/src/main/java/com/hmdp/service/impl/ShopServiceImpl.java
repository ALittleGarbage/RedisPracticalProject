package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        Shop shop = cacheClient.queryWithPenetration(
                RedisConstants.CACHE_SHOP_KEY,
                id,
                Shop.class,
                this::getById,
                30L, TimeUnit.MINUTES);

        //缓存击穿基于互斥锁
        /*Shop shop = cacheClient.queryWithBreakdownByMutex(
                RedisConstants.CACHE_SHOP_KEY,
                id,
                Shop.class,
                this::getById,
                30L, TimeUnit.SECONDS);*/

        /*//缓存击穿利用逻辑过期时间
        Shop shop = cacheClient.queryWithBreakdownByExpireTime(
                RedisConstants.CACHE_SHOP_KEY,
                id,
                Shop.class,
                this::getById,
                30L, TimeUnit.SECONDS);*/

        //返回商铺信息
        if (shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

/*    //解决缓存击穿，通过逻辑过期解决
    private Shop querySolveCacheBreakdownByExpireTime(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //从Redis查询商铺缓存
        String redisDataShopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在，为null和空字符串""的情况
        if (StrUtil.isBlank(redisDataShopJson)) {
            //不存在，就不是热点key，一般热点商铺都会提前存入Redis，返回错误信息
            return null;
        }
        //json转对象
        RedisData redisData = JSONUtil.toBean(redisDataShopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        //判断是否过期
        if(!LocalDateTime.now().isAfter(redisData.getExpireTime())){
            //未过期，返回
            return shop;
        }

        String lock = RedisConstants.LOCK_SHOP_KEY + id;
        //获取互斥锁成功
        if(tryLock(lock)){
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //注意！需要进行双重检查，因为线程b在线程a重建缓存前，判断shop过期
                    //在线程a释放锁后，因为某种原因线程b才开始获取互斥锁，导致重复重建缓存
                    //重建缓存
                    addShop2Redis(shop.getId(), 30L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lock);
                }
            });
        }
        //无论是否过期都返回
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private void addShop2Redis(Long id, Long expireSeconds){
        //查询shop信息
        Shop shop = getById(id);
        //封装过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入Redis
        stringRedisTemplate.opsForValue().set(
                RedisConstants.CACHE_SHOP_KEY + id,
                JSONUtil.toJsonStr(redisData));

    }

    //解决缓存击穿，通过互斥锁解决
    private Shop querySolveCacheBreakdownByMutex(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在，不为null和空字符串""的情况
        if (StrUtil.isNotBlank(shopJson)) {
            //存在，返回信息
            return JSONUtil.toBean(shopJson, Shop.class);

        }
        //命中的是否为空字符串
        if ("".equals(shopJson)) {
            //返回错误信息
            return null;
        }

        Shop shop = null;
        String lock = RedisConstants.LOCK_SHOP_KEY + id;
        try {
            //尝试获取互斥锁
            boolean isLock = tryLock(lock);
            //获取失败
            if (!isLock){
                Thread.sleep(50);
                //递归，直到能够获取到shop
                return querySolveCacheBreakdownByMutex(id);
            }
            //为null不存在，根据id查询
            shop = getById(id);
            //id不存在，返回错误信息
            if (shop == null) {
                //将空值写入Redis
                stringRedisTemplate.opsForValue().set(key,//key
                        "", //空值
                        RedisConstants.CACHE_NULL_TTL,// 2
                        TimeUnit.MINUTES); //分钟
                //返回空信息
                return null;
            }
            //id存在，将数据写入Redis，设置有效期
            stringRedisTemplate.opsForValue().set(key, //key
                    JSONUtil.toJsonStr(shop), // value
                    RedisConstants.CACHE_SHOP_TTL, // 30
                    TimeUnit.MINUTES); //分钟
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁，finally的执行早于try里面的return
            unlock(lock);
        }

        return shop;
    }


    //解决缓存穿透
    private Shop querySolveCachePenetration(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在，不为null和空字符串""的情况
        if (StrUtil.isNotBlank(shopJson)) {
            //存在，返回信息
            return JSONUtil.toBean(shopJson, Shop.class);

        }
        //命中的是否为空字符串
        if ("".equals(shopJson)) {
            //返回错误信息
            return null;
        }
        //为null不存在，根据id查询
        Shop shop = getById(id);
        //id不存在，返回空信息
        if (shop == null) {
            //将空值写入Redis
            stringRedisTemplate.opsForValue().set(key,//key
                    "", //空值
                    RedisConstants.CACHE_NULL_TTL,// 2
                    TimeUnit.MINUTES); //分钟
            //返回空信息
            return null;
        }
        //id存在，将数据写入Redis，设置有效期
        stringRedisTemplate.opsForValue().set(key, //key
                JSONUtil.toJsonStr(shop), // value
                RedisConstants.CACHE_SHOP_TTL, // 30
                TimeUnit.MINUTES); //分钟

        return shop;
    }

    private boolean tryLock(String key){
        //尝试获取互斥锁，设置有效时间
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,
                "1",
                10, //设置10秒
                TimeUnit.SECONDS);
        //判断是否获取锁
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        //解锁，删除key
        stringRedisTemplate.delete(key);
    }*/

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        //判断商铺是否存在
        if (id == null) {
            //返回错误信息
            return Result.fail("商铺id不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存，下次用再取
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        //返回成功信息
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //如果坐标为null，则直接返回数据
        if (x == null || y == null) {
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<Shop>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        int from = (current-1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;


        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        RedisConstants.SHOP_GEO_KEY + typeId,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5 * 1000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs()
                                .includeDistance()
                                .limit(end));//截取0~end个数据
        //如果为空的情况
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();
        int size = content.size();
        //截取的数据有可能为空
        if (from >= size){
            return Result.ok(Collections.emptyList());
        }

        //截取from~end的数据
        List<Long> shopIds = new ArrayList<>(size);
        List<Double> distances = new ArrayList<>(size);
        content.stream().skip(from).forEach(result -> {
            //获取shopId
            String shopIdStr = result.getContent().getName();
            shopIds.add(Long.valueOf(shopIdStr));
            //获取距离
            double distance = result.getDistance().getValue();
            distances.add(distance);
        });

        String join = StrUtil.join(",", shopIds);
        //根据shopId，获得按照顺序的shopList列表
        List<Shop> shopList = query()
                .in("id", shopIds)
                .last("ORDER BY FIELD(id," + join + ")").list();
        //将距离放入shop中
        for (int i = 0; i < shopList.size(); i++) {
            shopList.get(i).setDistance(distances.get(i));
        }

        return Result.ok(shopList);
    }
}