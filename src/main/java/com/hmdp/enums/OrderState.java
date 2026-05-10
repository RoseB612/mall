package com.hmdp.enums;

/**
 * 订单状态枚举
 * 1：未支付；2：已支付；3：已核销；4：已取消；5：退款中；6：已退款
 */
public enum OrderState {
    UNPAID(1, "未支付"),
    PAID(2, "已支付"),
    USED(3, "已核销"),
    CANCELED(4, "已取消"),
    REFUNDING(5, "退款中"),
    REFUNDED(6, "已退款");

    private final int code;
    private final String desc;

    OrderState(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static OrderState fromCode(int code) {
        for (OrderState state : OrderState.values()) {
            if (state.getCode() == code) {
                return state;
            }
        }
        return null;
    }
}
