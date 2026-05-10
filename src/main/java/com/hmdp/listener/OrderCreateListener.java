package com.hmdp.listener;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 异步订单创建监听器
 * 负责从 RabbitMQ 中获取订单消息，并进行数据库持久化落库
 */
@Slf4j
@Component
public class OrderCreateListener {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = RabbitMQConfig.ORDER_CREATE_QUEUE)
    public void listenOrderCreate(VoucherOrder voucherOrder) {
        log.info("接收到异步订单创建消息：orderId={}", voucherOrder.getId());
        try {
            // 执行实际的数据库落库业务（内含分布式锁和事务）
            voucherOrderService.handleVoucherOrder(voucherOrder);
            log.info("异步订单创建成功：orderId={}", voucherOrder.getId());
        } catch (Exception e) {
            log.error("异步订单创建失败：orderId={}", voucherOrder.getId(), e);
            // 这里抛出异常以便让 RabbitMQ 进行重试或进入死信队列
            throw e;
        }
    }
}
