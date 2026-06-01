package com.hmdp.service.impl;

import com.alibaba.cola.statemachine.StateMachine;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.RabbitMQConfig;
import com.hmdp.dto.PaymentStatusCheckMessage;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderEvent;
import com.hmdp.enums.OrderState;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;

/**
 * 券订单服务实现。
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final int MAX_PAYMENT_STATUS_CHECK_RETRY = 3;
    private static final int PAYMENT_STATUS_CHECK_DELAY_MS = 30 * 1000;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Resource
    @org.springframework.context.annotation.Lazy
    private IVoucherOrderService voucherOrderService;

    @Resource
    private StateMachine<OrderState, OrderEvent, VoucherOrder> orderStateMachine;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        int code = result == null ? 0 : result.intValue();
        if (code != 0) {
            return Result.fail(code == 1 ? "库存不足" : "不能重复下单");
        }

        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        order.setStatus(OrderState.UNPAID.getCode());
        order.setCreateTime(LocalDateTime.now());

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.ORDER_CREATE_EXCHANGE,
                RabbitMQConfig.ORDER_CREATE_ROUTING_KEY,
                order
        );
        log.info("[VoucherOrderService] 异步订单创建消息已投递，orderId={}", orderId);

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.ORDER_DELAYED_EXCHANGE,
                RabbitMQConfig.ORDER_ROUTING_KEY,
                orderId,
                message -> {
                    message.getMessageProperties().setDelay(RabbitMQConfig.ORDER_DELAY_MS);
                    return message;
                }
        );
        log.info("[VoucherOrderService] 订单超时消息已投递，orderId={}，将在{}ms后触发关单", orderId, RabbitMQConfig.ORDER_DELAY_MS);

        return Result.ok(orderId);
    }

    @Override
    public void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("[VoucherOrderService] 不允许重复下单，userId={}", userId);
            return;
        }
        try {
            voucherOrderService.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId())
                .count();
        if (count > 0) {
            log.error("[VoucherOrderService] 用户已购买过该券，userId={}, voucherId={}", userId, voucherOrder.getVoucherId());
            return;
        }

        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("[VoucherOrderService] 库存不足，voucherId={}", voucherOrder.getVoucherId());
            return;
        }

        if (voucherOrder.getStatus() == null) {
            voucherOrder.setStatus(OrderState.UNPAID.getCode());
        }
        save(voucherOrder);
    }

    @Override
    public boolean cancelOrder(Long orderId) {
        RLock lock = redissonClient.getLock("lock:order:state:" + orderId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.warn("[VoucherOrderService] 关单任务获取锁失败，可能有并发支付操作，orderId={}", orderId);
            schedulePaymentStatusCheck(orderId, 0, PAYMENT_STATUS_CHECK_DELAY_MS);
            return false;
        }
        try {
            VoucherOrder order = getById(orderId);
            if (order == null) {
                return false;
            }
            OrderState currentState = OrderState.fromCode(order.getStatus());
            if (currentState == OrderState.CANCELED) {
                return true;
            }
            if (currentState != OrderState.UNPAID && currentState != OrderState.PAYING) {
                log.warn("[VoucherOrderService] 当前状态不允许取消，orderId={}, status={}", orderId, currentState);
                return false;
            }
            fireEvent(order, OrderEvent.CANCEL);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 取消订单异常，orderId={}", orderId, e);
            return false;
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Override
    public boolean markOrderPaying(Long orderId) {
        RLock lock = redissonClient.getLock("lock:order:state:" + orderId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.warn("[VoucherOrderService] 标记支付中获取锁失败，orderId={}", orderId);
            schedulePaymentStatusCheck(orderId, 0, PAYMENT_STATUS_CHECK_DELAY_MS);
            return false;
        }
        try {
            VoucherOrder order = getById(orderId);
            if (order == null) {
                return false;
            }
            OrderState currentState = OrderState.fromCode(order.getStatus());
            if (currentState == OrderState.PAYING) {
                return true;
            }
            if (currentState != OrderState.UNPAID) {
                log.warn("[VoucherOrderService] 当前状态不允许进入支付中，orderId={}, status={}", orderId, currentState);
                return false;
            }
            fireEvent(order, OrderEvent.START_PAY);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 标记订单支付中异常，orderId={}", orderId, e);
            return false;
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Override
    public boolean payOrder(Long orderId) {
        RLock lock = redissonClient.getLock("lock:order:state:" + orderId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.warn("[VoucherOrderService] 支付回调获取锁失败，转入补偿确认。orderId={}", orderId);
            schedulePaymentStatusCheck(orderId, 0, PAYMENT_STATUS_CHECK_DELAY_MS);
            return false;
        }
        try {
            VoucherOrder order = getById(orderId);
            if (order == null) {
                return false;
            }
            OrderState currentState = OrderState.fromCode(order.getStatus());
            if (currentState == OrderState.PAID) {
                return true;
            }
            if (currentState != OrderState.UNPAID && currentState != OrderState.PAYING) {
                log.warn("[VoucherOrderService] 当前状态不允许支付成功，orderId={}, status={}", orderId, currentState);
                return false;
            }
            fireEvent(order, OrderEvent.PAY);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 支付订单异常，orderId={}", orderId, e);
            return false;
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Override
    public void handlePaymentStatusCheck(Long orderId, int retryCount) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            log.warn("[VoucherOrderService] 支付状态确认时订单不存在，orderId={}", orderId);
            return;
        }

        OrderState state = OrderState.fromCode(order.getStatus());
        if (state == null) {
            log.warn("[VoucherOrderService] 支付状态确认时发现未知状态，orderId={}, status={}", orderId, order.getStatus());
            return;
        }

        if (state == OrderState.PAID || state == OrderState.CANCELED || state == OrderState.USED
                || state == OrderState.REFUNDING || state == OrderState.REFUNDED) {
            log.info("[VoucherOrderService] 订单已进入终态或后置流程，无需继续确认。orderId={}, status={}", orderId, state);
            return;
        }

        if (state == OrderState.UNPAID) {
            boolean canceled = cancelOrder(orderId);
            if (!canceled && retryCount < MAX_PAYMENT_STATUS_CHECK_RETRY) {
                schedulePaymentStatusCheck(orderId, retryCount + 1, PAYMENT_STATUS_CHECK_DELAY_MS);
            }
            return;
        }

        if (state == OrderState.PAYING) {
            if (retryCount >= MAX_PAYMENT_STATUS_CHECK_RETRY) {
                log.warn("[VoucherOrderService] 支付状态多次未确认，按超时关单处理。orderId={}, retryCount={}", orderId, retryCount);
                boolean canceled = cancelOrder(orderId);
                if (!canceled) {
                    schedulePaymentStatusCheck(orderId, retryCount + 1, PAYMENT_STATUS_CHECK_DELAY_MS);
                }
                return;
            }

            // TODO: 接入第三方查单后，这里改为“查单成功则置为已支付，未支付则关单，未知则继续补偿”
            log.info("[VoucherOrderService] 支付状态暂未确认，稍后重试。orderId={}, retryCount={}", orderId, retryCount);
            schedulePaymentStatusCheck(orderId, retryCount + 1, PAYMENT_STATUS_CHECK_DELAY_MS);
        }
    }

    @Override
    public boolean useOrder(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return false;
        }
        try {
            fireEvent(order, OrderEvent.USE);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 核销订单异常，orderId={}", orderId, e);
            return false;
        }
    }

    @Override
    public boolean applyRefund(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return false;
        }
        try {
            fireEvent(order, OrderEvent.REFUND_APPLY);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 申请退款异常，orderId={}", orderId, e);
            return false;
        }
    }

    @Override
    public boolean refundSuccess(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return false;
        }
        try {
            fireEvent(order, OrderEvent.REFUND_SUCCESS);
            return true;
        } catch (Exception e) {
            log.error("[VoucherOrderService] 退款成功异常，orderId={}", orderId, e);
            return false;
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void fireEvent(VoucherOrder order, OrderEvent event) {
        OrderState currentState = OrderState.fromCode(order.getStatus());
        if (currentState == null) {
            throw new IllegalStateException("未知订单状态: " + order.getStatus());
        }
        orderStateMachine.fireEvent(currentState, event, order);
    }

    private void schedulePaymentStatusCheck(Long orderId, int retryCount, int delayMs) {
        PaymentStatusCheckMessage payload = new PaymentStatusCheckMessage(orderId, retryCount);
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.ORDER_DELAYED_EXCHANGE,
                RabbitMQConfig.PAYMENT_STATUS_CHECK_ROUTING_KEY,
                payload,
                message -> {
                    message.getMessageProperties().setDelay(delayMs);
                    return message;
                }
        );
    }
}
