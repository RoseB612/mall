package com.hmdp.listener;

import com.hmdp.config.RabbitMQConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * 缓存清理死信消费者（异常感知闭环的终极防线）
 *
 * 场景：CacheEvictListener 经过 5 次指数退避重试（历时约12分钟）依然失败。
 *      消息被 RabbitMQ 自动路由至 cache.evict.dlq 死信队列。
 * 作用：接收死信消息，触发高级别监控告警（如钉钉/企业微信机器人通知），
 *      提醒运维和开发人员人工介入，彻底杜绝异常被吞没。
 */
@Slf4j
@Component
public class CacheEvictDlqListener {

    @RabbitListener(queues = RabbitMQConfig.CACHE_EVICT_DLQ)
    public void listenCacheEvictDlq(String shopId) {
        log.error("==================== 严重级别告警 ====================");
        log.error("[DLQ] 缓存清理任务已耗尽 5 次重试，彻底失败！");
        log.error("[DLQ] 涉及商铺ID: {}", shopId);
        log.error("[DLQ] 请立即排查 Redis 服务状态或网络连通性，并手动清理缓存避免脏数据！");
        log.error("====================================================");

        // 真实业务场景下，这里通常会：
        // 1. 调用钉钉/企微 Webhook 接口发送群告警
        // 2. 写入数据库中的死信日志表，提供给后台管理系统进行重置补偿
        // 3. 对接 Prometheus / 阿里云监控 触发电话告警
    }
}
