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

        builder.externalTransition()
                .from(OrderState.UNPAID)
                .to(OrderState.PAYING)
                .on(OrderEvent.START_PAY)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        // 兼容现有直接支付成功的调用路径
        builder.externalTransition()
                .from(OrderState.UNPAID)
                .to(OrderState.PAID)
                .on(OrderEvent.PAY)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        builder.externalTransition()
                .from(OrderState.PAYING)
                .to(OrderState.PAID)
                .on(OrderEvent.PAY)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        builder.externalTransition()
                .from(OrderState.UNPAID)
                .to(OrderState.CANCELED)
                .on(OrderEvent.CANCEL)
                .when(checkCondition())
                .perform(orderStateAction.cancelOrderAction());

        builder.externalTransition()
                .from(OrderState.PAYING)
                .to(OrderState.CANCELED)
                .on(OrderEvent.CANCEL)
                .when(checkCondition())
                .perform(orderStateAction.cancelOrderAction());

        builder.externalTransition()
                .from(OrderState.PAID)
                .to(OrderState.USED)
                .on(OrderEvent.USE)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        builder.externalTransition()
                .from(OrderState.PAID)
                .to(OrderState.REFUNDING)
                .on(OrderEvent.REFUND_APPLY)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        builder.externalTransition()
                .from(OrderState.REFUNDING)
                .to(OrderState.REFUNDED)
                .on(OrderEvent.REFUND_SUCCESS)
                .when(checkCondition())
                .perform(orderStateAction.updateStateAction());

        return builder.build(MACHINE_ID);
    }

    private com.alibaba.cola.statemachine.Condition<VoucherOrder> checkCondition() {
        return context -> context != null;
    }
}
