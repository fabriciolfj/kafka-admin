package com.github.fabriciolfj.kafkaadmin.noduplicities;

import com.github.fabriciolfj.kafkaadmin.util.AppConfigs;
import com.github.fabriciolfj.kafkaadmin.util.AppConfigs2;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/*
* Para não duplicar mensagens, em caso de falha, deve: habilitar idempotence e utilizar uma transação.
* exemplo: se 2 produtores, emitir a mesma mensagem, usando o mesmo id transação, um será abordado.
* */

@Slf4j
public class HelloProducer {

    public static void main(String[] args) {
        log.info("Create kafka Producer...");

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs2.applicationId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs2.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfigs2.transaction_id);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        producer.initTransactions();

        log.info("Start FIRST transaction...");
        producer.beginTransaction();
        try {
            for (int i = 0; i < AppConfigs2.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs2.topicName, i, "simple message-t1-" + i));
                producer.send(new ProducerRecord<>(AppConfigs2.topicName, i, "simple message-t1-" + i));
            }
            log.info("Committing first transaction");
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException();
        }

        log.info("Start SECOND transaction...");
        producer.beginTransaction();
        try {
            for (int i = 0; i < AppConfigs2.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs2.topicName, i, "simple message-t2-" + i));
                producer.send(new ProducerRecord<>(AppConfigs2.topicName, i, "simple message-t2-" + i));
            }
            log.info("Aborting second transaction");
            producer.abortTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException();
        }

        log.info("Finished sending messags. Closing Producer.");
        producer.close();
    }
}
