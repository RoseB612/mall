package com.hmdp.enums;

/**
 * 订单事件枚举
 */
public enum OrderEvent {
    PAY("支付"),
    CANCEL("取消"),
    USE("核销"),
    REFUND_APPLY("申请退款"),
    REFUND_SUCCESS("退款成功");

    private final String desc;

    OrderEvent(String desc) {
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }
}
