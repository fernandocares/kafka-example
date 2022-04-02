package br.com.fernandocares.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService {

    public static void main(String[] args) throws InterruptedException {
        var emailService = new EmailService();
        try(var kafkaConsumerService = new KafkaConsumerService<>(
                EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::consume,
                String.class,
                Map.of()
                )) {
            kafkaConsumerService.run();
        }
    }

    private void consume(ConsumerRecord<String, String> record) throws InterruptedException {
        System.out.println("----------------------------------------------------");
        System.out.println("Sending Email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        Thread.sleep(1000);

        System.out.println("Email sended");
    }

}
