package com.hmdp.service.impl;

import com.hmdp.config.RabbitMQConfig;
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
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.alibaba.cola.statemachine.StateMachine;
import com.hmdp.enums.OrderEvent;
import com.hmdp.enums.OrderState;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 */
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

    /** 注入 RabbitTemplate，用于向 RabbitMQ 发送延迟消息 */
    @Resource
    private RabbitTemplate rabbitTemplate;

    /**
     * 脚本初始化
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    @Resource
    @org.springframework.context.annotation.Lazy
    private IVoucherOrderService voucherOrderService;

    public void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //3.获取锁
        boolean isLock = lock.tryLock();
        //4.判断是否获取锁成功
        if(!isLock) {
            //失败，返回错误或重试
           log.error("不允许重复下单");
           return;
        }
        try {
            //通过代理对象调用，触发spring aop的事务管理
            voucherOrderService.createVoucherOrder(voucherOrder);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        }finally {
            //释放锁
            lock.unlock();
        }
    }
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //2.判断结果是否为0
        int r = 0;
        if (result != null) {
            r = result.intValue();
        }
        if(r!=0){
            //2.1.不为0，代表没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }
        // 2. 将订单信息发送到 RabbitMQ 异步落库队列
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.ORDER_CREATE_EXCHANGE,
                RabbitMQConfig.ORDER_CREATE_ROUTING_KEY,
                order
        );
        log.info("[VoucherOrderService] 异步订单创建消息已投递，orderId={}", orderId);

        // 3. 向 RabbitMQ 延迟交换机发送消息，通过 x-delay 消息头精确控制延迟时间
        //    原理：插件收到消息后先缓存起来，x-delay 毫秒到期后再投递到 order.timeout.queue
        //    优点：每条消息独立计时，互不影响（解决了DLX+TTL方案的队头阻塞问题）
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.ORDER_DELAYED_EXCHANGE,   // 延迟交换机
                RabbitMQConfig.ORDER_ROUTING_KEY,        // 路由 Key
                orderId,                                  // 消息体：订单ID
                message -> {
                    // 通过消息头 x-delay 设置延迟时间（单位：毫秒）
                    // 这是插件方案的核心：交换机读取这个 Header 决定何时投递
                    message.getMessageProperties().setDelay(RabbitMQConfig.ORDER_DELAY_MS);
                    return message;
                }
        );
        log.info("[VoucherOrderService] 订单超时消息已投递，orderId={}，将在{}ms后触发关单",
                orderId, RabbitMQConfig.ORDER_DELAY_MS);



        // 4. 返回订单号给前端
        return Result.ok(orderId);
    }


//    public Result seckillVoucher(Long voucherId) {
//        //查询用户券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //判断秒杀时间
//        //是否开始
//        LocalDateTime beginTime = voucher.getBeginTime();
//        if(beginTime.isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始！");
//        }
//        //是否结束
//        LocalDateTime endTime = voucher.getEndTime();
//        if(endTime.isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        //判断库存呢是否充足
//        if(voucher.getStock()<=0){
//            return Result.fail("库存不足！");
//        }
//        Long userId = UserHolder.getUser().getId();
//       //创建锁对象
//        //SimpleRedisLock  lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        //判断是否获取锁成功
//        if(!isLock) {
//            //失败，返回错误或重试
//            return Result.fail("不允许重复下单");
//
//        }
//        try {
//            //直接调用，不会触发spring aop的事务管理
//            //要通过代理调用，获取代理对象，才会被spring aop拦截
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        }finally {
//            //释放锁
//            lock.unlock();
//        }
//
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        //查询订单
        Long userId =voucherOrder.getUserId();
            int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            //判断是否存在
            if (count > 0) {
                //用户已经购买过了
                log.error("用户已经购买过一次了");
                return;
            }
            //扣减库存
            boolean success = seckillVoucherService
                    .update()
                    .setSql("stock=stock-1")
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0)
                    .update();
            if (!success) {
                log.error("库存不足");
                return ;
            }
            save(voucherOrder);
    }

    @Resource
    private StateMachine<OrderState, OrderEvent, VoucherOrder> orderStateMachine;

    @Override
    public boolean cancelOrder(Long orderId) {
        // 1. Redisson 粗粒度前置拦截
        RLock lock = redissonClient.getLock("lock:order:state:" + orderId);
        // 尝试获取锁，不阻塞等待。如果获取失败，说明正有其他线程（如支付回调）在处理该订单
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.warn("[VoucherOrderService] 关单任务获取锁失败，可能有并发支付操作，直接跳过，orderId={}", orderId);
            return false;
        }
        try {
            VoucherOrder order = getById(orderId);
            if (order == null) return false;
            // 2. 状态机内部有 MySQL 乐观锁细粒度兜底
            fireEvent(order, OrderEvent.CANCEL);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 取消订单异常，orderId={}", orderId, e);
            return false;
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Override
    public boolean payOrder(Long orderId) {
        // 1. Redisson 粗粒度前置拦截
        RLock lock = redissonClient.getLock("lock:order:state:" + orderId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.warn("[VoucherOrderService] 支付回调获取锁失败，可能有并发关单操作，直接跳过，orderId={}", orderId);
            return false;
        }
        try {
            VoucherOrder order = getById(orderId);
            if (order == null) return false;
            // 2. 状态机内部有 MySQL 乐观锁细粒度兜底
            fireEvent(order, OrderEvent.PAY);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 支付订单异常，orderId={}", orderId, e);
            return false;
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Override
    public boolean useOrder(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) return false;
        try {
            fireEvent(order, OrderEvent.USE);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 核销订单异常，orderId={}", orderId, e);
            return false;
        }
    }

    @Override
    public boolean applyRefund(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) return false;
        try {
            fireEvent(order, OrderEvent.REFUND_APPLY);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 申请退款异常，orderId={}", orderId, e);
            return false;
        }
    }

    @Override
    public boolean refundSuccess(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) return false;
        try {
            fireEvent(order, OrderEvent.REFUND_SUCCESS);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 退款成功异常，orderId={}", orderId, e);
            return false;
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void fireEvent(VoucherOrder order, OrderEvent event) {
        // 1. 获取当前状态
        OrderState currentState = OrderState.fromCode(order.getStatus());
        // 2. 触发状态机
        orderStateMachine.fireEvent(currentState, event, order);
    }
}
