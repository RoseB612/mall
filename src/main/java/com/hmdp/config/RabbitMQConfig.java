package com.hmdp.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ 队列拓扑配置。
 */
@Configuration
public class RabbitMQConfig {

    public static final String ORDER_DELAYED_EXCHANGE = "order.delayed.exchange";
    public static final String ORDER_TIMEOUT_QUEUE = "order.timeout.queue";
    public static final String ORDER_ROUTING_KEY = "order.timeout";
    public static final int ORDER_DELAY_MS = 15 * 60 * 1000;

    public static final String ORDER_CREATE_EXCHANGE = "order.create.exchange";
    public static final String ORDER_CREATE_QUEUE = "order.create.queue";
    public static final String ORDER_CREATE_ROUTING_KEY = "order.create";

    public static final String PAYMENT_STATUS_CHECK_QUEUE = "payment.status.check.queue";
    public static final String PAYMENT_STATUS_CHECK_ROUTING_KEY = "payment.status.check";

    public static final String CACHE_EVICT_EXCHANGE = "cache.evict.exchange";
    public static final String CACHE_EVICT_QUEUE = "cache.evict.queue";
    public static final String CACHE_EVICT_ROUTING_KEY = "cache.evict";
    public static final String CACHE_EVICT_DLX = "cache.evict.dlx";
    public static final String CACHE_EVICT_DLQ = "cache.evict.dlq";
    public static final String CACHE_EVICT_DEAD_KEY = "cache.evict.dead";

    @Bean
    public CustomExchange orderDelayedExchange() {
        return new CustomExchange(
                ORDER_DELAYED_EXCHANGE,
                "x-delayed-message",
                true,
                false,
                Collections.singletonMap("x-delayed-type", "direct")
        );
    }

    @Bean
    public Queue orderTimeoutQueue() {
        return QueueBuilder.durable(ORDER_TIMEOUT_QUEUE).build();
    }

    @Bean
    public Binding orderTimeoutBinding(Queue orderTimeoutQueue, CustomExchange orderDelayedExchange) {
        return BindingBuilder
                .bind(orderTimeoutQueue)
                .to(orderDelayedExchange)
                .with(ORDER_ROUTING_KEY)
                .noargs();
    }

    @Bean
    public Queue paymentStatusCheckQueue() {
        return QueueBuilder.durable(PAYMENT_STATUS_CHECK_QUEUE).build();
    }

    @Bean
    public Binding paymentStatusCheckBinding(Queue paymentStatusCheckQueue, CustomExchange orderDelayedExchange) {
        return BindingBuilder
                .bind(paymentStatusCheckQueue)
                .to(orderDelayedExchange)
                .with(PAYMENT_STATUS_CHECK_ROUTING_KEY)
                .noargs();
    }

    @Bean
    public DirectExchange orderCreateExchange() {
        return new DirectExchange(ORDER_CREATE_EXCHANGE);
    }

    @Bean
    public Queue orderCreateQueue() {
        return QueueBuilder.durable(ORDER_CREATE_QUEUE).build();
    }

    @Bean
    public Binding orderCreateBinding(Queue orderCreateQueue, DirectExchange orderCreateExchange) {
        return BindingBuilder.bind(orderCreateQueue).to(orderCreateExchange).with(ORDER_CREATE_ROUTING_KEY);
    }

    @Bean
    public DirectExchange cacheEvictExchange() {
        return new DirectExchange(CACHE_EVICT_EXCHANGE);
    }

    @Bean
    public Queue cacheEvictQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", CACHE_EVICT_DLX);
        args.put("x-dead-letter-routing-key", CACHE_EVICT_DEAD_KEY);
        return QueueBuilder.durable(CACHE_EVICT_QUEUE).withArguments(args).build();
    }

    @Bean
    public Binding cacheEvictBinding(Queue cacheEvictQueue, DirectExchange cacheEvictExchange) {
        return BindingBuilder.bind(cacheEvictQueue).to(cacheEvictExchange).with(CACHE_EVICT_ROUTING_KEY);
    }

    @Bean
    public DirectExchange cacheEvictDlx() {
        return new DirectExchange(CACHE_EVICT_DLX);
    }

    @Bean
    public Queue cacheEvictDlq() {
        return QueueBuilder.durable(CACHE_EVICT_DLQ).build();
    }

    @Bean
    public Binding cacheEvictDlqBinding(Queue cacheEvictDlq, DirectExchange cacheEvictDlx) {
        return BindingBuilder.bind(cacheEvictDlq).to(cacheEvictDlx).with(CACHE_EVICT_DEAD_KEY);
    }
}
