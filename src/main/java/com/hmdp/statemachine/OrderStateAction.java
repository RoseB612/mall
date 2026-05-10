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

@Slf4j
@Component
public class OrderStateAction {

    @Resource
    @Lazy
    private IVoucherOrderService voucherOrderService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    /**
     * 通用的状态更新操作：直接按预期源状态更新为目标状态
     */
    public Action<OrderState, OrderEvent, VoucherOrder> updateStateAction() {
        return (from, to, event, order) -> {
            boolean success = voucherOrderService.update()
                    .set("status", to.getCode())
                    .eq("id", order.getId())
                    .eq("status", from.getCode())
                    .update();
            if (!success) {
                log.warn("[OrderStateMachine] 状态流转失败：并发冲突或状态不匹配，orderId={}, from={}, to={}", 
                        order.getId(), from, to);
                throw new RuntimeException("状态流转失败");
            }
            log.info("[OrderStateMachine] 状态流转成功：orderId={}, from={}, to={}", 
                    order.getId(), from, to);
        };
    }

    /**
     * 取消订单操作：更新状态并回滚库存
     */
    public Action<OrderState, OrderEvent, VoucherOrder> cancelOrderAction() {
        return (from, to, event, order) -> {
            // 1. 乐观锁更新状态
            boolean success = voucherOrderService.update()
                    .set("status", to.getCode())
                    .eq("id", order.getId())
                    .eq("status", from.getCode())
                    .update();
            if (!success) {
                log.warn("[OrderStateMachine] 取消订单失败：状态已被修改，orderId={}", order.getId());
                throw new RuntimeException("取消订单失败，状态已被修改");
            }

            // 2. 恢复库存
            boolean restoreSuccess = seckillVoucherService.update()
                    .setSql("stock = stock + 1")
                    .eq("voucher_id", order.getVoucherId())
                    .update();

            if (!restoreSuccess) {
                log.error("[OrderStateMachine] 取消订单恢复库存失败！orderId={}, voucherId={}", 
                        order.getId(), order.getVoucherId());
                throw new RuntimeException("恢复库存失败，触发事务回滚");
            }

            log.info("[OrderStateMachine] 订单已取消，库存已恢复。orderId={}", order.getId());
        };
    }
}
