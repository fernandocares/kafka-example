package br.com.fernandocares.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();

        try(var kafkaConsumerService = new KafkaConsumerService<>(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::consume,
                Order.class,
                Map.of()
                )) {
            kafkaConsumerService.run();
        }
    }

    private final KafkaDispatcherService<Order> kafkaDispatcherService = new KafkaDispatcherService<>();

    void consume(ConsumerRecord<String, Order> record) throws InterruptedException, ExecutionException {
        System.out.println("----------------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        Thread.sleep(5000);

        var order = record.value();

        if(isFraud(order)) {
            // pretending that fraud happens
            System.out.println("Order is a FRAUD!");
            kafkaDispatcherService.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
        } else {
            System.out.println("Order checked: " + order);
            kafkaDispatcherService.send("ECOMMERCE_ORDER_APPROVED", order.getUserId(), order);
        }

    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
