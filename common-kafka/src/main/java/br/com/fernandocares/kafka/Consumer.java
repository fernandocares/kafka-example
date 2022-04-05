package br.com.fernandocares.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Consumer<T> {

    void consume(ConsumerRecord<String, T> record) throws Exception;

}
