package com.hmdp.listener;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

/**
 * 订单超时关闭监听器（基于 RabbitMQ 死信队列）
 *
 * 工作原理：
 *   1. 下单时，orderId 被发往"暂存队列"（TTL=15分钟，无消费者）。
 *   2. 15分钟后，消息在暂存队列里超时，变成"死信"。
 *   3. RabbitMQ 自动将死信路由到"死信队列"（order.dead.queue）。
 *   4. 本类的 @RabbitListener 监听死信队列，被框架自动调用，处理关单逻辑。
 *
 * 与 Redisson 版本的对比：
 *   - Redisson版：自己管理后台线程（while+take）+ Redis ZSet
 *   - RabbitMQ版：@RabbitListener 注解，框架管理线程，代码更简洁
 *   - 可靠性更高：消息持久化存储在 RabbitMQ，服务宕机重启后消息不丢失
 */
@Slf4j
@Component
public class OrderTimeoutListener {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    /**
     * 监听死信队列，处理超时订单。
     *
     * @RabbitListener 注解说明：
     *   - queues：监听的队列名（死信队列，真正接收超时消息的地方）
     *   - 框架会自动维护消费者线程，无需手写 while(true)
     *   - 方法执行完后框架自动 ACK；如果抛出异常，框架会执行 NACK + 重试
     *
     * @param orderId 超时的订单ID（由 RabbitMQ 从死信队列取出并反序列化）
     */
    @RabbitListener(queues = RabbitMQConfig.ORDER_TIMEOUT_QUEUE)
    @Transactional(rollbackFor = Exception.class)
    public void handleOrderTimeout(Long orderId) {
        log.info("[OrderTimeoutListener] 收到超时订单，orderId={}", orderId);

        // 1. 查询当前订单的最新状态
        VoucherOrder order = voucherOrderService.getById(orderId);

        // 2. 防御性判断：订单不存在，直接跳过
        if (order == null) {
            log.warn("[OrderTimeoutListener] 订单不存在，忽略。orderId={}", orderId);
            return;
        }

        // 3. 幂等判断：只有状态为"1-未支付"时才执行关单
        //    status: 1=未支付 2=已支付 3=已核销 4=已取消
        //    如果用户已在15分钟内完成支付，status已变为2，直接忽略，避免误杀订单
        if (order.getStatus() != 1) {
            log.info("[OrderTimeoutListener] 订单已处理（status={}），无需关单。orderId={}",
                    order.getStatus(), orderId);
            return;
        }

        // 4. 通过状态机关闭订单（内部会自动处理状态校验、状态变更及库存恢复）
        boolean success = voucherOrderService.cancelOrder(orderId);

        if (!success) {
            log.warn("[OrderTimeoutListener] 关单失败，订单可能已被支付或取消。orderId={}", orderId);
            return;
        }

        log.info("[OrderTimeoutListener] 超时关单成功。orderId={}", orderId);
    }
}
