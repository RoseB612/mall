package com.hmdp.listener;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.hmdp.config.RabbitMQConfig;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Canal Binlog 监听器（独立守护线程）
 *
 * 作用：伪装成 MySQL 从节点，实时接收 tb_shop 表的变更（UPDATE/DELETE），
 *      一旦有变更，立即执行 Redis 缓存删除。
 *      如果删除失败，则将任务投递给 RabbitMQ 进行指数退避重试，形成完整的异常防御闭环。
 */
@Slf4j
@Component
public class CanalBinlogListener {

    @Value("${canal.host:192.168.203.128}")
    private String canalHost;

    @Value("${canal.port:11111}")
    private Integer canalPort;

    @Value("${canal.destination:dianping}")
    private String destination;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RabbitTemplate rabbitTemplate;

    @PostConstruct
    public void startListener() {
        // 开启独立线程不断拉取 Canal 数据，避免阻塞 Spring 启动线程
        Thread thread = new Thread(() -> {
            CanalConnector connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(canalHost, canalPort),
                    destination,
                    "",
                    ""
            );
            
            while (true) {
                try {
                    connector.connect();
                    // 订阅特定表的 binlog，例如只关心 dianping 库下的 tb_shop 表
                    connector.subscribe("dianping\\.tb_shop");
                    connector.rollback();

                    log.info("[Canal] 成功连接至 Canal Server，开始监听 binlog...");

                    while (true) {
                        // 每次拉取 100 条 binlog 变更记录
                        Message message = connector.getWithoutAck(100);
                        long batchId = message.getId();
                        int size = message.getEntries().size();

                        if (batchId == -1 || size == 0) {
                            // 没有数据，休眠 1 秒后继续拉取
                            Thread.sleep(1000);
                        } else {
                            // 处理 binlog
                            handleEntries(message.getEntries());
                        }
                        connector.ack(batchId); // 确认消费成功
                    }
                } catch (Exception e) {
                    log.error("[Canal] 监听发生异常，3秒后尝试重连...", e);
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException ignored) {}
                } finally {
                    connector.disconnect();
                }
            }
        });
        thread.setName("Canal-Binlog-Listener");
        thread.setDaemon(true); // 设为守护线程，随主线程退出
        thread.start();
    }

    private void handleEntries(List<CanalEntry.Entry> entries) throws Exception {
        for (CanalEntry.Entry entry : entries) {
            // 我们只关心 ROWDATA 类型的数据（即真实的增删改）
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            CanalEntry.EventType eventType = rowChange.getEventType();

            // 对于缓存一致性，我们只关心数据被修改（UPDATE）或者删除（DELETE）
            if (eventType == CanalEntry.EventType.UPDATE || eventType == CanalEntry.EventType.DELETE) {
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    // 获取修改后的列数据（或者删除前的列数据）
                    List<CanalEntry.Column> columns = (eventType == CanalEntry.EventType.UPDATE) 
                            ? rowData.getAfterColumnsList() 
                            : rowData.getBeforeColumnsList();

                    // 找出商铺 ID
                    String shopId = getColumnValue(columns, "id");
                    if (shopId != null) {
                        log.info("[Canal] 监听到 tb_shop 表发生 {} 操作，商铺ID: {}", eventType, shopId);
                        evictCacheWithRetry(shopId);
                    }
                }
            }
        }
    }

    private String getColumnValue(List<CanalEntry.Column> columns, String columnName) {
        for (CanalEntry.Column column : columns) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return column.getValue();
            }
        }
        return null;
    }

    /**
     * 执行缓存淘汰，并在失败时投递到 RabbitMQ 做补偿
     */
    private void evictCacheWithRetry(String shopId) {
        String redisKey = RedisConstants.CACHE_SHOP_KEY + shopId;
        try {
            // 1. 删除 Redis 缓存
            stringRedisTemplate.delete(redisKey);
            
            log.info("[Canal] 缓存清理成功，商铺ID: {}", shopId);
        } catch (Exception e) {
            log.error("[Canal] 缓存清理失败！准备将任务投递至 MQ 补偿队列...", e);
            // 核心闭环：清理失败不要紧，交给 MQ 做指数退避重试
            try {
                rabbitTemplate.convertAndSend(
                        RabbitMQConfig.CACHE_EVICT_EXCHANGE,
                        RabbitMQConfig.CACHE_EVICT_ROUTING_KEY,
                        shopId
                );
                log.info("[Canal] 补偿任务已成功投递至 MQ，商铺ID: {}", shopId);
            } catch (Exception mqEx) {
                log.error("[Canal] 极端异常！MQ 投递也失败了！商铺ID: {}", shopId, mqEx);
                // 此时可以写入本地磁盘日志，或直接触发最基础的告警
            }
        }
    }
}
