package com.hmdp.statemachine;

import com.alibaba.cola.statemachine.Action;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderEvent;
import com.hmdp.enums.OrderState;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;

@Slf4j
@Component
public class OrderStateAction {

    @Resource
    @Lazy
    private IVoucherOrderService voucherOrderService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    public Action<OrderState, OrderEvent, VoucherOrder> updateStateAction() {
        return (from, to, event, order) -> {
            boolean success = voucherOrderService.update()
                    .set("status", to.getCode())
                    .set(to == OrderState.PAID, "pay_time", LocalDateTime.now())
                    .eq("id", order.getId())
                    .eq("status", from.getCode())
                    .update();
            if (!success) {
                log.warn("[OrderStateMachine] 状态流转失败，orderId={}, from={}, to={}", order.getId(), from, to);
                throw new RuntimeException("状态流转失败");
            }
            log.info("[OrderStateMachine] 状态流转成功，orderId={}, from={}, to={}", order.getId(), from, to);
        };
    }

    public Action<OrderState, OrderEvent, VoucherOrder> cancelOrderAction() {
        return (from, to, event, order) -> {
            boolean success = voucherOrderService.update()
                    .set("status", to.getCode())
                    .eq("id", order.getId())
                    .eq("status", from.getCode())
                    .update();
            if (!success) {
                log.warn("[OrderStateMachine] 取消订单失败，状态已变化，orderId={}", order.getId());
                throw new RuntimeException("取消订单失败，状态已变化");
            }

            boolean restoreSuccess = seckillVoucherService.update()
                    .setSql("stock = stock + 1")
                    .eq("voucher_id", order.getVoucherId())
                    .update();
            if (!restoreSuccess) {
                log.error("[OrderStateMachine] 取消订单恢复库存失败，orderId={}, voucherId={}", order.getId(), order.getVoucherId());
                throw new RuntimeException("恢复库存失败");
            }

            log.info("[OrderStateMachine] 订单已取消，库存已恢复。orderId={}", order.getId());
        };
    }
}
