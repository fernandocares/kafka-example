package br.com.fernandocares.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService {

    public static void main(String[] args) throws InterruptedException {
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

    void consume(ConsumerRecord<String, Order> record) throws InterruptedException {
        System.out.println("----------------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        Thread.sleep(5000);

        System.out.println("Order checked");
    }
}
