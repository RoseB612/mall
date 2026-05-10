package com.hmdp.listener;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 缓存清理重试消费者（MQ 指数退避重试）
 *
 * 场景：Canal 监听到 Binlog 后尝试清理 Redis 失败，将任务投递至此队列。
 * 作用：利用 RabbitMQ 的 ack 机制和 yaml 中配置的 ExponentialBackOff（指数退避），
 *      对缓存清理任务进行最大 5 次（1s/5s/25s/125s/625s）的重试。
 */
@Slf4j
@Component
public class CacheEvictListener {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ShopServiceImpl shopService;

    @RabbitListener(queues = RabbitMQConfig.CACHE_EVICT_QUEUE)
    public void listenCacheEvict(String shopId) {
        log.info("[CacheEvictListener] 收到缓存清理补偿任务，准备重试。商铺ID: {}", shopId);

        String redisKey = RedisConstants.CACHE_SHOP_KEY + shopId;
        
        try {
            // 1. 补偿清理 Redis 缓存
            stringRedisTemplate.delete(redisKey);

            log.info("[CacheEvictListener] 缓存清理补偿任务执行成功！商铺ID: {}", shopId);
        } catch (Exception e) {
            log.error("[CacheEvictListener] 缓存清理重试失败！抛出异常触发 RabbitMQ 指数退避... 商铺ID: {}", shopId, e);
            // 抛出异常，RabbitMQ 监听到异常后会给 broker 发送 nack，
            // 配合 application.yaml 中的 retry 配置，自动进行指数退避重试。
            // 当重试次数超过 max-attempts (5) 时，消息会被路由到死信交换机。
            throw e;
        }
    }
}
