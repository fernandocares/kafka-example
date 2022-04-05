package br.com.fernandocares.kafka;

import br.com.fernandocares.kafka.deserializers.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaConsumerService<T> implements Closeable {

    private final KafkaConsumer<String, T> kafka;
    private final Consumer consumer;

    public KafkaConsumerService(String groupId, String topic, Consumer consumer, Class<T> deserializationType, Map<String,String> properties) {
        this(groupId, consumer, deserializationType, properties);
        kafka.subscribe(Collections.singletonList(topic));
    }

    public KafkaConsumerService(String groupId, Pattern topic, Consumer consumer, Class<T> deserializationType, Map<String,String> properties) {
        this(groupId, consumer, deserializationType, properties);
        kafka.subscribe(topic);
    }

    public KafkaConsumerService(String groupId, Consumer consumer, Class<T> deserializationType, Map<String,String> properties) {
        this.kafka = new KafkaConsumer<>(getProperties(groupId, deserializationType, properties));
        this.consumer = consumer;
    }

    void run() {
        while(true) {
            var records = kafka.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("MENSAGEM ENCONTRADA");

                for (var record : records) {
                    try {
                        consumer.consume(record);
                    } catch (Exception e) {
                        // TODO handle with exception
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public Properties getProperties(String groupId, Class<T> deserializationType, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, deserializationType.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        kafka.close();
    }
}
