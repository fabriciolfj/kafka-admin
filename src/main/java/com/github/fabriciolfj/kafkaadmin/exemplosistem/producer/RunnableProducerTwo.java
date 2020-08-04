package com.github.fabriciolfj.kafkaadmin.exemplosistem.producer;

import com.github.fabriciolfj.kafkaadmin.exemplosistem.datagenerator.InvoiceGenerator;
import com.github.fabriciolfj.kafkaadmin.exemplosistem.domain.PosInvoice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class RunnableProducerTwo implements Runnable {

    private KafkaProducer<String, PosInvoice> producer;
    private Long speed;
    private String topic;
    private InvoiceGenerator invoiceGenerator;
    private Integer quantity;

    public RunnableProducerTwo(KafkaProducer<String, PosInvoice> producer, Long speed, String topic, Integer quantity) {
        this.producer = producer;
        this.speed = speed;
        this.topic = topic;
        this.quantity = quantity;
        this.invoiceGenerator = InvoiceGenerator.getInstance();
    }

    @Override
    public void run() {
        for (int i = 0; i <  quantity; i++) {
            try {
                PosInvoice nextInvoice = this.invoiceGenerator.getNextInvoice();var message = new ProducerRecord<String, PosInvoice>(topic, nextInvoice.getPosID(), nextInvoice);
                this.producer.send(message);
                Thread.sleep(speed);
                log.info("Send message: " + nextInvoice.getPosID());
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }

        }
    }
}
