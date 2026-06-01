package com.hmdp.dto;

import java.io.Serializable;

/**
 * 支付状态确认补偿消息。
 */
public class PaymentStatusCheckMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long orderId;
    private Integer retryCount;

    public PaymentStatusCheckMessage() {
    }

    public PaymentStatusCheckMessage(Long orderId, Integer retryCount) {
        this.orderId = orderId;
        this.retryCount = retryCount;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }
}
