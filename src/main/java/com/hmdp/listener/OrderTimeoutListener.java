package com.hmdp.listener;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderState;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

/**
 * 订单超时关闭监听器。
 */
@Slf4j
@Component
public class OrderTimeoutListener {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = RabbitMQConfig.ORDER_TIMEOUT_QUEUE)
    @Transactional(rollbackFor = Exception.class)
    public void handleOrderTimeout(Long orderId) {
        log.info("[OrderTimeoutListener] 收到超时订单，orderId={}", orderId);

        VoucherOrder order = voucherOrderService.getById(orderId);
        if (order == null) {
            log.warn("[OrderTimeoutListener] 订单不存在，忽略。orderId={}", orderId);
            return;
        }

        OrderState currentState = OrderState.fromCode(order.getStatus());
        if (currentState == null) {
            log.warn("[OrderTimeoutListener] 未识别订单状态，orderId={}, status={}", orderId, order.getStatus());
            return;
        }

        if (currentState == OrderState.PAYING) {
            log.info("[OrderTimeoutListener] 订单仍在支付中，转入支付状态确认补偿。orderId={}", orderId);
            voucherOrderService.handlePaymentStatusCheck(orderId, 0);
            return;
        }

        if (currentState != OrderState.UNPAID) {
            log.info("[OrderTimeoutListener] 订单已处理，无需关单。orderId={}, status={}", orderId, currentState);
            return;
        }

        boolean success = voucherOrderService.cancelOrder(orderId);
        if (!success) {
            log.warn("[OrderTimeoutListener] 关单失败，订单可能已被支付或正在补偿。orderId={}", orderId);
            return;
        }

        log.info("[OrderTimeoutListener] 超时关单成功。orderId={}", orderId);
    }
}
