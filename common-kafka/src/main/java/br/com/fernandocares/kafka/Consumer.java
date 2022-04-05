package br.com.fernandocares.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public interface Consumer<T> {

    void consume(ConsumerRecord<String, T> record) throws InterruptedException, ExecutionException;

}
