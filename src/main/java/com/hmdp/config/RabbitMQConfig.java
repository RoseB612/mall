package com.hmdp.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ 队列拓扑配置
 *
 * 包含两套队列体系：
 *   1. 订单超时队列（延迟插件方案）：处理秒杀订单15分钟未支付自动关闭
 *   2. 缓存淘汰补偿队列（Canal 失败兜底）：Canal 淘汰缓存失败时的 MQ 补偿 + DLQ 告警
 *
 * 缓存淘汰队列拓扑图：
 *
 *   Canal 淘汰失败
 *     │
 *     ▼
 *   [cache.evict.exchange]  ← DirectExchange（Canal 失败时投递到这里）
 *     │
 *     ▼
 *   [cache.evict.queue]  ← 指数退避重试（最多5次：1s/5s/25s/125s/625s）
 *     │ 消费5次仍失败
 *     ▼
 *   [cache.evict.dlx]   ← 死信交换机（自动接收超过重试的消息）
 *     │
 *     ▼
 *   [cache.evict.dlq]  ← 死信队列（CacheEvictDlqListener 监听，触发告警）
 */
@Configuration
public class RabbitMQConfig {

    // ==================== 订单超时队列常量 ====================

    /** 延迟交换机名称（使用插件提供的 x-delayed-message 类型）*/
    public static final String ORDER_DELAYED_EXCHANGE = "order.delayed.exchange";

    /** 订单超时队列（消费者直接监听这里）*/
    public static final String ORDER_TIMEOUT_QUEUE = "order.timeout.queue";

    /** 路由 Key */
    public static final String ORDER_ROUTING_KEY = "order.timeout";

    /** 订单超时时间：15 分钟（毫秒），发消息时放入 x-delay 消息头 */
    public static final int ORDER_DELAY_MS = 15 * 60 * 1000;

    /** 异步订单创建交换机 */
    public static final String ORDER_CREATE_EXCHANGE = "order.create.exchange";

    /** 异步订单创建队列 */
    public static final String ORDER_CREATE_QUEUE = "order.create.queue";

    /** 异步订单创建路由 Key */
    public static final String ORDER_CREATE_ROUTING_KEY = "order.create";

    // ==================== 缓存淘汰补偿队列常量 ====================

    /** 缓存淘汰主交换机：Canal 失败时向此处投递补偿消息 */
    public static final String CACHE_EVICT_EXCHANGE = "cache.evict.exchange";

    /** 缓存淘汰主队列：CacheEvictListener 监听，指数退避重试 */
    public static final String CACHE_EVICT_QUEUE = "cache.evict.queue";

    /** 缓存淘汰路由 Key */
    public static final String CACHE_EVICT_ROUTING_KEY = "cache.evict";

    /** 死信交换机：接收超过最大重试次数的死信消息 */
    public static final String CACHE_EVICT_DLX = "cache.evict.dlx";

    /** 死信队列：CacheEvictDlqListener 监听，触发钉钉/企微告警 */
    public static final String CACHE_EVICT_DLQ = "cache.evict.dlq";

    /** 死信路由 Key */
    public static final String CACHE_EVICT_DEAD_KEY = "cache.evict.dead";

    // ==================== 订单超时队列 Bean ====================

    /**
     * 延迟交换机：类型为 x-delayed-message（插件提供）。
     * 消息发来时，插件先把消息缓存在 Mnesia 数据库中，
     * 等 x-delay 毫秒过去之后，再按 x-delayed-type 指定的路由方式投递到队列。
     */
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

    // ==================== 缓存淘汰补偿队列 Bean ====================

    /** 缓存淘汰主交换机 */
    @Bean
    public DirectExchange cacheEvictExchange() {
        return new DirectExchange(CACHE_EVICT_EXCHANGE);
    }

    /**
     * 缓存淘汰主队列。
     * 关键配置：绑定死信交换机（x-dead-letter-exchange）。
     * 当消息经过 max-attempts 次重试仍失败时，Spring AMQP 拒绝消息（NACK + 不重入队列），
     * RabbitMQ 自动将其路由到指定的死信交换机，进而流入死信队列。
     */
    @Bean
    public Queue cacheEvictQueue() {
        Map<String, Object> args = new HashMap<>();
        // 绑定死信交换机：消息超过重试次数后自动转发到此
        args.put("x-dead-letter-exchange", CACHE_EVICT_DLX);
        // 死信路由 Key
        args.put("x-dead-letter-routing-key", CACHE_EVICT_DEAD_KEY);
        return QueueBuilder.durable(CACHE_EVICT_QUEUE).withArguments(args).build();
    }

    @Bean
    public Binding cacheEvictBinding(Queue cacheEvictQueue, DirectExchange cacheEvictExchange) {
        return BindingBuilder.bind(cacheEvictQueue).to(cacheEvictExchange).with(CACHE_EVICT_ROUTING_KEY);
    }

    /** 死信交换机：接收所有超过重试上限的消息 */
    @Bean
    public DirectExchange cacheEvictDlx() {
        return new DirectExchange(CACHE_EVICT_DLX);
    }

    /** 死信队列：监听此队列并触发告警 */
    @Bean
    public Queue cacheEvictDlq() {
        return QueueBuilder.durable(CACHE_EVICT_DLQ).build();
    }

    @Bean
    public Binding cacheEvictDlqBinding(Queue cacheEvictDlq, DirectExchange cacheEvictDlx) {
        return BindingBuilder.bind(cacheEvictDlq).to(cacheEvictDlx).with(CACHE_EVICT_DEAD_KEY);
    }
}


/**
 * RabbitMQ 延迟插件方案配置
 *
 * 使用 rabbitmq_delayed_message_exchange 官方插件实现延迟投递。
 *
 * 与 DLX + TTL 方案的对比：
 *   DLX+TTL：需要两个Exchange + 两个Queue，拓扑复杂；且存在"队头阻塞"风险
 *            （不同TTL的消息会互相阻塞，但本项目所有消息都是15min所以影响不大）
 *   插件方案：只需一个 x-delayed-message 类型的 Exchange + 一个 Queue，
 *            每条消息独立计时，互不影响，天然解决队头阻塞问题。
 *
 * 队列拓扑图（极其简单）：
 *
 *  生产者（带 x-delay=900000 消息头）
 *    │
 *    ▼
 *  [延迟交换机: order.delayed.exchange]  ← 类型: x-delayed-message
 *    │  插件在交换机内部缓存消息，x-delay 毫秒到期后再路由
 *    ▼
 *  [订单超时队列: order.timeout.queue]  ← 消费者在这里监听
 */
@Configuration
public class RabbitMQConfig {

    // ==================== 常量定义 ====================

    /** 延迟交换机名称（使用插件提供的 x-delayed-message 类型）*/
    public static final String ORDER_DELAYED_EXCHANGE = "order.delayed.exchange";

    /** 订单超时队列（消费者直接监听这里）*/
    public static final String ORDER_TIMEOUT_QUEUE = "order.timeout.queue";

    /** 路由 Key */
    public static final String ORDER_ROUTING_KEY = "order.timeout";

    /** 订单超时时间：15 分钟（毫秒），发消息时放入 x-delay 消息头 */
    public static final int ORDER_DELAY_MS = 15 * 60 * 1000;

    /** 异步订单创建交换机 */
    public static final String ORDER_CREATE_EXCHANGE = "order.create.exchange";

    /** 异步订单创建队列 */
    public static final String ORDER_CREATE_QUEUE = "order.create.queue";

    /** 异步订单创建路由 Key */
    public static final String ORDER_CREATE_ROUTING_KEY = "order.create";

    // ==================== Exchange 声明 ====================

    /**
     * 延迟交换机：类型为 x-delayed-message（插件提供）。
     *
     * 使用 CustomExchange 而非 DirectExchange，因为这是非标准类型。
     * 参数 x-delayed-type 指定底层真实的路由类型（这里用 direct 精准路由）。
     *
     * 工作原理：
     *   消息发来时，插件先把消息缓存在 Mnesia 数据库（RabbitMQ 内置存储）中。
     *   等 x-delay 毫秒过去之后，再按 x-delayed-type 指定的路由方式投递到队列。
     */
    @Bean
    public CustomExchange orderDelayedExchange() {
        return new CustomExchange(
                ORDER_DELAYED_EXCHANGE,
                "x-delayed-message",        // 插件提供的特殊 Exchange 类型
                true,                        // durable：持久化
                false,                       // auto-delete：不自动删除
                Collections.singletonMap("x-delayed-type", "direct") // 底层路由类型
        );
    }


    // ==================== Queue 声明 ====================

    /** 订单超时队列：普通持久化队列，消费者直接监听 */
    @Bean
    public Queue orderTimeoutQueue() {
        return QueueBuilder.durable(ORDER_TIMEOUT_QUEUE).build();
    }


    // ==================== 绑定关系 ====================

    /** 把超时队列绑定到延迟交换机 */
    @Bean
    public Binding orderTimeoutBinding(Queue orderTimeoutQueue, CustomExchange orderDelayedExchange) {
        return BindingBuilder
                .bind(orderTimeoutQueue)
                .to(orderDelayedExchange)
                .with(ORDER_ROUTING_KEY)
                .noargs();
    }

    // ==================== 异步订单创建配置 ====================

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
}
