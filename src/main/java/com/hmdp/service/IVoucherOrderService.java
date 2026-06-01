package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderEvent;

/**
 * 券订单服务。
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    void createVoucherOrder(VoucherOrder voucherOrder);

    void handleVoucherOrder(VoucherOrder voucherOrder);

    boolean cancelOrder(Long orderId);

    /**
     * 标记订单进入支付中。
     */
    boolean markOrderPaying(Long orderId);

    boolean payOrder(Long orderId);

    /**
     * 处理支付状态确认补偿消息。
     */
    void handlePaymentStatusCheck(Long orderId, int retryCount);

    boolean useOrder(Long orderId);

    boolean applyRefund(Long orderId);

    boolean refundSuccess(Long orderId);

    void fireEvent(VoucherOrder order, OrderEvent event);
}
