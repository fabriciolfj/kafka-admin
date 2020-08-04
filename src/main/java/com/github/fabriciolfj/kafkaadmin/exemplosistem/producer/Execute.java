package com.github.fabriciolfj.kafkaadmin.exemplosistem.producer;

import com.github.fabriciolfj.kafkaadmin.exemplosistem.domain.PosInvoice;
import com.github.fabriciolfj.kafkaadmin.exemplosistem.serde.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Execute {

    public static void main(String[] args) {
        String topicName = "invoice";

        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig3.applicationId);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig3.bootStrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> kafkaProducer = new KafkaProducer<>(properties);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final List<RunnableProducer> runnableProducers = new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            RunnableProducer runnableProducer = new RunnableProducer(i, kafkaProducer, topicName, 200);
            runnableProducers.add(runnableProducer);
            executor.submit(runnableProducer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (RunnableProducer p : runnableProducers) p.shutdown();
            executor.shutdown();
            log.info("Closing Executor Service");
            try {
                executor.awaitTermination(200 * 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

    }
}
