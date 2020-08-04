package com.github.fabriciolfj.kafkaadmin.multithreadproducer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;

@Slf4j
public class Dispatcher implements Runnable {

    private String topicName;
    private KafkaProducer<Integer, String> producer;

    public Dispatcher(String topicName, KafkaProducer<Integer, String> producer) {
        this.topicName = topicName;
        this.producer = producer;
    }

    @Override
    public void run() {
        log.info("Start Processing");

        for(int i = 0; i <  10; i ++) {
            producer.send(new ProducerRecord<>(topicName, null, UUID.randomUUID().toString()));
        }

        log.info("Finished sending");
    }
}
