package com.hmdp.enums;

/**
 * 订单状态枚举。
 *
 * 为了兼容现有数据库状态值，新增的“支付中”状态使用新编码 7，
 * 避免影响已存在的“已支付/已取消/已退款”等状态语义。
 */
public enum OrderState {
    UNPAID(1, "未支付"),
    PAID(2, "已支付"),
    USED(3, "已核销"),
    CANCELED(4, "已取消"),
    REFUNDING(5, "退款中"),
    REFUNDED(6, "已退款"),
    PAYING(7, "支付中");

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
