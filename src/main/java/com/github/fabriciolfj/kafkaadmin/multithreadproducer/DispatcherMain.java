package com.github.fabriciolfj.kafkaadmin.multithreadproducer;

import com.github.fabriciolfj.kafkaadmin.util.AppConfigs;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class DispatcherMain {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        Thread[] dispatchers = new Thread[30];
        log.info("Starting dispatcher threads...");

        for (int i = 0; i < 30; i++) {
            dispatchers[i] = new Thread(new Dispatcher(AppConfigs.topicName, producer));
            dispatchers[i].start();
        }

        try {
            for (Thread t: dispatchers) t.join();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        } finally {
            producer.close();
            log.info("Finished");
        }
    }
}
