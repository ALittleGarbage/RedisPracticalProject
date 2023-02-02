package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    //全局唯一ID
    @Resource
    private RedisIdWorker redisIdWorker;
    //redisson分布式锁
    @Resource
    private RedissonClient redissonClient;
    //代理对象
    private IVoucherOrderService proxy;

    //初始化lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        //设置脚本位置
        SECKILL_SCRIPT.setLocation(new ClassPathResource("lua/seckillStream.lua"));
        //设置返回值类型
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //初始化单线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //在spring容器初始化的时候执行该方法
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        private final String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //获取消息队列中的订单信息，没有订单则阻塞等待2秒
                    //XREADGROUP GROUP c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //获取失败，进入下次循环
                        continue;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //获取成功，则处理订单
                    handleVoucherOrder(voucherOrder);
                    //完成ACK确认
                    //SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }

            }
        }
        //pending-list检查
        private void handlePendingList() {
            while (true) {
                try {
                    //获取pending-list中的订单信息
                    //XREADGROUP GROUP c1 COUNT 1 STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //获取失败，pending-list中没有异常消息，结束循环
                        break;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //获取成功，处理异常的订单
                    handleVoucherOrder(voucherOrder);
                    //完成ACK确认
                    //SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    /*//初始化阻塞队列
    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //获取队列的订单信息，没有订单则阻塞等待
                    VoucherOrder voucherOrder = orderTasks.take();
                    //处理订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }

            }
        }
    }*/

    //主方法
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));

        //异常信息，返回失败
        if (result != 0){
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }

        //获取主线程代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //执行lua脚本
        Long id = UserHolder.getUser().getId();
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), id.toString());
        //异常信息，返回失败
        if (result != 0){
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }

        Long userId = UserHolder.getUser().getId();
        //获取主线程代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);
        //加入到阻塞队列中
        orderTasks.add(voucherOrder);

        return Result.ok(orderId);
    }*/

    //再次进行逻辑判断，互斥的处理订单
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock isLock = redissonClient.getLock("lock:order:" + userId);
        //尝试获取锁
        boolean flag = isLock.tryLock();
        if (!flag){
            log.error("不能重复下单");
            return;
        }
        //获取锁成功
        try {
            //生成一人一单的订单
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            isLock.unlock();
        }

    }
    //操作数据库，生成订单
    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //检查一人一单
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0){
            log.error("用户已经购买过一次");
            return;
        }
        //减扣库存
        //set stock = stock-1 where voucher_id = ? and stock > 0
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success){
            log.error("库存不足");
            return;
        }
        //从数据库生成订单
        save(voucherOrder);
    }

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //判断时间
        LocalDateTime now = LocalDateTime.now();
        if(now.isBefore(voucher.getBeginTime()) || now.isAfter(voucher.getEndTime()) ){
            return Result.fail("未在规定时间内");
        }
        //检查库存
        if (voucher.getStock() < 1){
            return Result.fail("库存不足");
        }

        Long userId = UserHolder.getUser().getId();
        //创建锁对象
        //SimpleRedisLock isLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock isLock = redissonClient.getLock("lock:order:" + userId);
        //尝试获取锁
        boolean flag = isLock.tryLock();
        if (!flag){
            //获取锁失败，返回错误
            return Result.fail("不允许重复下单");
        }
        //获取锁成功
        try {
            //this调用会导致事务不生效，可以通过AopContext获取到Proxy对象,从而实现代理.
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            //生成一人一单的订单
            return proxy.createVoucherOrder(voucherId);
        } finally {
            //释放锁
            isLock.unlock();
        }
    }*/
}
