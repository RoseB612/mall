package com.hmdp.service.impl;

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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    /**
     * 脚本初始化
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1. 获取队列中的订单信息
                    VoucherOrder order = orderTasks.take();
                    // 2. 创建订单
                    handleVoucherOrder(order);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }
   /* private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的队列信息
                    VoucherOrder order = orderTasks.take();
                    //2.创建订单
                    handleVoucherOrder(order);

                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                }

            }

        }
    }*/

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
                //直接调用，不会触发spring aop的事务管理
                //要通过代理调用，获取代理对象，才会被spring aop拦截
                proxy.createVoucherOrder(voucherOrder);
            } catch (IllegalStateException e) {
                throw new RuntimeException(e);
            }finally {
                //释放锁
                lock.unlock();
            }
        }
    private IVoucherOrderService proxy;
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
        // 2. 将订单信息放入阻塞队列
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        orderTasks.add(order);

        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 3. 返回订单号给前端
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
}
