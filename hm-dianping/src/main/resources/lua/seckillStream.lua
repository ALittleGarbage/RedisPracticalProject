local voucherId = ARGV[1];  --优惠券id
local userId = ARGV[2];     --用户id
local orderId = ARGV[3];    --订单id

--拼接key
local stockKey = 'seckill:stock:' .. voucherId;
local orderKey = 'seckill:order:' .. voucherId;

--判断库存是否充足
if (tonumber(redis.call('get', stockKey)) <= 0) then
    --库存不足，返回1
    return 1;
end
--判断用户是否已经下单
if (redis.call('sismember', orderKey, userId) == 1) then
    --用户已经下单，返回2
    return 2;
end
--减扣库存
redis.call('incrby', stockKey, -1);
--将用户id插入到set合集中
redis.call('sadd', orderKey, userId);

--发送消息到队列中，XADD stream.orders * k1 v1 k2 v2 ···
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

return 0;