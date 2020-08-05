package com.github.fabriciolfj.kafkaadmin.exemplosistemtwo;

import com.github.fabriciolfj.kafkaadmin.exemplosistemtwo.serde.JsonDeserializer;
import com.github.fabriciolfj.kafkaadmin.exemplosistemtwo.serde.JsonSerializer;
import com.github.fabriciolfj.kafkaadmin.exemplosistemtwo.types.PosInvoice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


@Slf4j
public class PosValidator {

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.groupID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer<String, PosInvoice>(consumerProps);
        consumer.subscribe(Arrays.asList(AppConfigs.sourceTopicNames));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<String, PosInvoice>(producerProps);

        while (true) {
            ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, PosInvoice> record: records) {
                if(record.value().getDeliveryAddress() != null && record.value().getDeliveryAddress().getContactNumber().equals("")) {
                    producer.send(new ProducerRecord<>(AppConfigs.invalidTopicName, record.value().getStoreID(), record.value()));
                    log.info("invalid record: " + record.value().getInvoiceNumber());
                } else {
                    log.info("valid record: " + record.value().getInvoiceNumber());
                    producer.send(new ProducerRecord<>(AppConfigs.validTopicName, record.value().getStoreID(), record.value()));
                }
            }
        }
    }
}
