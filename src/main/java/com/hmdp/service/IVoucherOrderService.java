package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.enums.OrderEvent;

/**
 * <p>
 *  服务类
 * </p>
 *
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    void createVoucherOrder(VoucherOrder voucherOrder);

    void handleVoucherOrder(VoucherOrder voucherOrder);

    /**
     * 取消订单（触发状态机）
     * @param orderId 订单ID
     * @return 是否成功
     */
    boolean cancelOrder(Long orderId);

    /**
     * 支付订单（触发状态机）
     * @param orderId 订单ID
     * @return 是否成功
     */
    boolean payOrder(Long orderId);

    /**
     * 核销订单
     */
    boolean useOrder(Long orderId);

    /**
     * 申请退款
     */
    boolean applyRefund(Long orderId);

    /**
     * 退款成功
     */
    boolean refundSuccess(Long orderId);

    /**
     * 触发状态机流转
     * @param order 订单实体
     * @param event 触发事件
     */
    void fireEvent(VoucherOrder order, OrderEvent event);
}
