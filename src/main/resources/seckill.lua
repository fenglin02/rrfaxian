--!参数脚本
--1.1.优惠券id
local voucherId=ARGV[1]
--1.2.用户id
local userId=ARGV[2]
--1.3.订单id
local orderId=ARGV[3]

--2,数据key
--2.1库存key
local stockKey='seckill:stock:'..voucherId
local orderKey='seckill:order:'..voucherId

--3.脚本业务
--3.1判断库存是否充足
if(tonumber(redis.call('get',stockKey))<=0) then
    --3.库存不足，返回1
    return 1
end
--3.2判断用户是否下单 sismember orderKey userId
if(redis.call('sismember',orderKey,userId)==1) then
    --存在说明重复下单,返回2
    return 2
end
--3.4扣库存 incrby stockKey -1
redis.call('incrby',stockKey,-1)
--3.5下单 存用户id sadd orederkey userId
redis.call('sadd',orderKey,userId)
--3.6发送消息到队列当中 XADD stream.oreder * k1 v1 k2 v2.....
redis.call('xadd','stream.order','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0;
