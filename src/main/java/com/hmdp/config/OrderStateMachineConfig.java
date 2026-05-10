package com.hmdp.config;

import com.alibaba.cola.statemachine.StateMachine;
import com.alibaba.cola.statemachine.builder.StateMachineBuilder;
import com.alibaba.cola.statemachine.builder.StateMachineBuilderFactory;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderEvent;
import com.hmdp.enums.OrderState;
import com.hmdp.statemachine.OrderStateAction;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

@Configuration
public class OrderStateMachineConfig {

    public static final String MACHINE_ID = "orderStateMachine";

    @Resource
    private OrderStateAction orderStateAction;

    @Bean
    public StateMachine<OrderState, OrderEvent, VoucherOrder> orderStateMachine() {
        StateMachineBuilder<OrderState, OrderEvent, VoucherOrder> builder = StateMachineBuilderFactory.create();

        // 1. 未支付 -> 支付 -> 已支付
        builder.externalTransition()
                .from(OrderState.UNPAID)
                .to(OrderState.PAID)
                .on(OrderEvent.PAY)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        // 2. 未支付 -> 取消 -> 已取消
        builder.externalTransition()
                .from(OrderState.UNPAID)
                .to(OrderState.CANCELED)
                .on(OrderEvent.CANCEL)
                .when(checkCondition())
                .perform(orderStateAction.cancelOrderAction());

        // 3. 已支付 -> 核销 -> 已核销
        builder.externalTransition()
                .from(OrderState.PAID)
                .to(OrderState.USED)
                .on(OrderEvent.USE)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        // 4. 已支付 -> 申请退款 -> 退款中
        builder.externalTransition()
                .from(OrderState.PAID)
                .to(OrderState.REFUNDING)
                .on(OrderEvent.REFUND_APPLY)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        // 5. 退款中 -> 退款成功 -> 已退款
        builder.externalTransition()
                .from(OrderState.REFUNDING)
                .to(OrderState.REFUNDED)
                .on(OrderEvent.REFUND_SUCCESS)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        return builder.build(MACHINE_ID);
    }

    private com.alibaba.cola.statemachine.Condition<VoucherOrder> checkCondition() {
        return (context) -> {
            return context != null;
        };
    }
}
