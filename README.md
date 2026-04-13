# 基于 SpringBoot 与 Redis 的高并发生活点评系统

<p align="center">
  <img src="https://img.shields.io/badge/SpringBoot-2.7+-green.svg" alt="SpringBoot">
  <img src="https://img.shields.io/badge/Redis-6.0+-red.svg" alt="Redis">
  <img src="https://img.shields.io/badge/MySQL-8.0+-blue.svg" alt="MySQL">
  <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License">
</p>

### 📖 项目简介
本项目是一个专注于高并发性能与分布式架构优化的生活点评与实地社交系统。项目深度集成了 Redis 缓存体系、分布式锁机制以及异步处理架构，致力于解决千兆级并发场景下的鉴权共享、缓存失效、库存超卖及 LBS 检索等核心技术难题，是一套具备大厂实践参考价值的高性能后端中台。

---

## 🌟 核心技术亮点 

- **身份认证**：采用 **JWT 双 Token 结合 Redis** 实现无状态分布式鉴权，并利用 **ThreadLocal** 封装用户信息，实现全链路 $O(1)$ 复杂度的上下文读取与内存泄露防护。
    
- **高可用保障**：构建 **Caffeine + Redis 二级缓存**架构，通过**布隆过滤器**拦截穿透请求，并配合**逻辑过期与分布式锁**从容应对缓存击穿与雪崩。
    
- **并发安全**：引入 **Redisson 分布式锁**接管全局互斥逻辑，利用 **WatchDog (看门狗)** 机制解决长事务锁过期隐患，保障多节点集群下“一人一单”规则的物理互斥。
    
- **秒杀引擎**：利用 **Redis + Lua 脚本**实现库存预检与原子流控，结合 **异步任务解耦**（本地 BlockingQueue 替代 MQ 轻量化处理）及 MySQL 行级锁 `WHERE stock > 0` 方案，在高并发下彻底根治超卖风险。

---

## 🌍 特色业务模块

### 1. LBS 地理位置社交
*   **高精度商户检索**：基于 **Redis GeoHash** 算法，实现百万级坐标位置的快速存储与 5km 范围内商户的毫秒级检索。
*   **智能距离排序**：动态计算用户与商户间的球面距离，优化 LBS 业务场景下的响应延迟。

### 2. 高性能自研 Feed 流
*   **关注体系设计**：基于 Redis **Set** 集合运算实现“共同关注”发现，提升社交裂变能力。
*   **推拉结合架构**：实现好友动态的毫秒级即时分发，确保信息流的低延迟展示。

---

## 🛠️ 技术栈清单

*   **核心框架**: Spring Boot 2.7, MyBatis-Plus
*   **缓存与分布式**: Redis, Redisson, Caffeine Cache
*   **异步机制**: Java BlockingQueue / 异步线程池
*   **数据库**: MySQL 8.0 (InnoDB)
*   **开发辅助**: Lombok, Hutool, Maven

---

## 📂 核心代码目录

```text
├── src/main/java/com/hmdp/
│   ├── controller/      # REST API 接口层
│   ├── service/         # 业务与核心引擎（秒杀、缓存逻辑）
│   ├── interceptor/     # 鉴权登录与 Token 刷新拦截器
│   └── utils/           # Redisson 锁、ID 生成器等工具类
├── src/main/resources/
│   ├── mapper/          # MyBatis SQL XML
│   └── seckill.lua      # Redis 秒杀扣减原子操作脚本
├── pom.xml              # 项目依赖配置
└── .gitignore           # 仓库忽略规则（过滤了敏感笔记和 DB 数据）
```

---

## 🚀 启动指南

1.  **环境准备**：安装 JDK 1.8+, MySQL 8.0+, Redis 6.2+。
2.  **配置数据库**：修改 `application.yaml` 中的 MySQL 与 Redis 连接参数。
3.  **运行项目**：启动 `HmDianPingApplication` 后，服务端默认运行在 `8081` 端口。

> [!NOTE]
> 这是一个全面展示高可用、高并发以及架构解耦能力的实战系统，不仅实现了业务逻辑闭环，更深入解决了分布式场景下的各类技术挑战。
