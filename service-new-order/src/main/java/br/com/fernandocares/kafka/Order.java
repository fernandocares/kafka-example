package br.com.fernandocares.kafka;

import java.math.BigDecimal;

public class Order {

    private final String userid, orderId;
    private final BigDecimal amount;

    public Order(String userid, String orderId, BigDecimal amount) {
        this.userid = userid;
        this.orderId = orderId;
        this.amount = amount;
    }
}
