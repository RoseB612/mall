package com.hmdp.listener;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.dto.PaymentStatusCheckMessage;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 支付状态确认补偿监听器。
 */
@Slf4j
@Component
public class PaymentStatusCheckListener {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = RabbitMQConfig.PAYMENT_STATUS_CHECK_QUEUE)
    public void handlePaymentStatusCheck(PaymentStatusCheckMessage message) {
        if (message == null || message.getOrderId() == null) {
            log.warn("[PaymentStatusCheckListener] 收到空的支付状态确认消息");
            return;
        }
        int retryCount = message.getRetryCount() == null ? 0 : message.getRetryCount();
        voucherOrderService.handlePaymentStatusCheck(message.getOrderId(), retryCount);
    }
}
