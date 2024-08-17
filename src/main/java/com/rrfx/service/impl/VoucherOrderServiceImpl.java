package com.rrfx.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.rrfx.dto.Result;
import com.rrfx.dto.UserDTO;
import com.rrfx.entity.VoucherOrder;
import com.rrfx.mapper.VoucherOrderMapper;
import com.rrfx.service.ISeckillVoucherService;
import com.rrfx.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.rrfx.utils.RedisIdWorker;
import com.rrfx.utils.UserHolder;
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
import java.util.concurrent.*;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
//    private ThreadPoolExecutor service=new ThreadPoolExecutor()
    IVoucherOrderService proxy;


    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName="stream.order";
        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 count 1 BLOCK 2000 STREAMS STREAM.ORDER >
                    //如果我们在处理消息的过程中崩溃，
                    // 我们的消息将保留在待处理条目列表中，
                    // 因此我们可以通过XREADGROUP最初提供 0 的 ID 并执行相同的循环来访问我们的历史记录。
                    // 一旦提供 0 的 ID，回复就是一组空的消息，
                    // 我们知道我们处理并确认了所有待处理消息：我们可以开始使用>作为 ID，以便获取新消息并重新加入正在处理新事物的消费者。
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断是否获取成功
                    //2.1如果获取失败,说明Pending-list没有异常消息,进行下一次循环
                    if(list==null||list.isEmpty()){
                        continue;
                    }
                    //解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    //3.如果获取成功处理订单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }
        private void handlePendingList(){
            while (true) {
                try {
                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 count 1 STREAMS STREAM.ORDER 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断是否获取成功
                    //2.1如果获取失败,说明没有消息,跳出handlePendingList循环
                    if(list==null||list.isEmpty()){
                        break;
                    }
                    //解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //3.如果获取成功处理订单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理Pending_list异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }


    /*private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的订单信息
                    VoucherOrder voucherOrder = ordersTasks.take();
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        //生成随机订单id（不会重复)
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),//key
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        //2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1不为0没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不允许重复下单");
        }
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);
    }

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        //2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1不为0没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不允许重复下单");
        }
        //2.2为0有购买资格,把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);
        //2.3放入阻塞队列
        ordersTasks.add(voucherOrder);
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);
    }*/

    /* @Override
     public Result seckillVoucher(Long voucherId) {
         //获取优惠券信息
         SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
         //是否过期
         LocalDateTime beginTime = voucher.getBeginTime();
         LocalDateTime endTime = voucher.getEndTime();
         if (endTime.isBefore(LocalDateTime.now())) {
             return Result.fail("优惠券过期!");
         }
         if (beginTime.isAfter(LocalDateTime.now())) {
             return Result.fail("秒杀尚未开始!");
         }
         if (voucher.getStock() < 1) {
             Result.fail("库存不足!");
         }
         Long userId = UserHolder.getUser().getId();
         RLock lock = redissonClient.getLock("lock:order:" + userId);
         boolean isLock = lock.tryLock();
         if(!isLock) {
             return Result.fail("不允许重复下单");
         }
         try {
             //获取代理对象
             IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
             return proxy.createVoucherOrder(voucherId);
         } finally {
             //释放锁
             lock.unlock();
         }

     }*/
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //判断用户是否已经有过订单
        //一人一单 超卖问题
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("不允许重复抢券！");
            return;
        }

        boolean success = seckillVoucherService.update().setSql("stock=stock-1").eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!success) {
            log.error("扣减失败!");
            return;
        }
        //创建订单
        save(voucherOrder);
    }
}
