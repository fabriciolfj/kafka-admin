package com.github.fabriciolfj.kafkaadmin.exemplesistemone.producer;

import com.github.fabriciolfj.kafkaadmin.exemplesistemone.domain.PosInvoice;
import com.github.fabriciolfj.kafkaadmin.exemplesistemone.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExecuteTwo {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig3.applicationId);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig3.bootStrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<String, PosInvoice>(properties);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        String topic = "invoice-two";

        for (int i = 0; i <  10; i++) {
            RunnableProducerTwo runnableProducerTwo = new RunnableProducerTwo(producer, 100L, topic, 34);
            executorService.submit(runnableProducerTwo);
        }

        executorService.awaitTermination(9000, TimeUnit.MILLISECONDS);
    }
}
