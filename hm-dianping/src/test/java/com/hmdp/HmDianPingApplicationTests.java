package com.hmdp;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
@RunWith(SpringRunner.class)
class HmDianPingApplicationTests {

    @Resource
    private IShopService shopService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisIdWorker redisIdWorker;

    private final ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    public void idWorkerTest() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            es.submit(() -> {
                for (int j = 0; j < 100; j++) {
                    long test = redisIdWorker.nextId("test");
                    System.out.println("test"+j+" = " + test);
                }
                countDownLatch.countDown();
            });

            countDownLatch.await();
        }

    }

    @Test
    public void setExpireTest() {
        Shop shop = shopService.getById(1);
        //封装过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(10));
        //写入Redis
        stringRedisTemplate.opsForValue().set(
                RedisConstants.CACHE_SHOP_KEY + "1",
                JSONUtil.toJsonStr(redisData));
    }


    @Resource
    private RedissonClient redissonClient;

    private RLock lock;

    /*@Resource
    private RedissonClient redissonClient2;

    @Resource
    private RedissonClient redissonClient3;


    //当前类中的每个@Test方法之前执行注解方法
    @BeforeEach
    void setUp() {
        RLock lock1 = redissonClient.getLock("lock");
        RLock lock2 = redissonClient2.getLock("lock");
        RLock lock3 = redissonClient3.getLock("lock");

        //创建联锁 multiLock
        lock = redissonClient.getMultiLock(lock1, lock2, lock3);
    }*/

    @Test
    void testRedisson() throws InterruptedException {
        //尝试获取锁，参数分别是：获取锁的最大等待时间（期间会重试），锁自动释放时间，时间单位
        boolean isLock = lock.tryLock(1,10, TimeUnit.SECONDS);
        //判断释放获取成功
        if(isLock) {
            try {
                System.out.println("执行业务");
            } finally {
                //释放锁
                lock.unlock();
            }
        }
    }

    @Test
    void testQueryShop2Redis() {
        List<Shop> list = shopService.list();
        //以typeId将shop分组
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //分批完成写入Redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //获取类型d
            Long typeId = entry.getKey();
            String key = RedisConstants.SHOP_GEO_KEY + typeId;
            //获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            //写入redis
            //GEOADD key 经纬度 member
            for (Shop shop : value) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(),shop.getY()))
                );
            }

            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

    @Test
    void testHyperLogLog() {
        //准岛数组，装用户数掘
        String[] users = new String[1000];
        //数组角标
        int index = 0;
        for(int i = 1; i <= 100000; i++){
            //赋值
            users[index++] = "user_" + i;
            //每1000条发送一次
            if(i%1000 == 0) {
                index = 0;
                stringRedisTemplate.opsForHyperLogLog().add("hll1",users);
            }
        }

        //统计数量
        Long size = stringRedisTemplate.opsForHyperLogLog().size("hll1");
        System.out.println("size = " + size);
    }
}
