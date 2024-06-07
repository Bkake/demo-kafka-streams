package com.siak.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class BankTransactionsProducer {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BankTransactionsProducer.class);
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ACKS_CONFIG, "all");
        props.put(RETRIES_CONFIG, "3");
        props.put(LINGER_MS_CONFIG, "1");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (Producer<String, String> producer = new KafkaProducer<>(props)){

            for (int i = 0; i < 200000 ; i++) {
                LOGGER.info("Producing batch: {} ", i);
                try {
                    producer.send(newRecordRandom("bkake"));
                    Thread.sleep(100);
                    producer.send(newRecordRandom("lama"));
                    Thread.sleep(100);
                    producer.send(newRecordRandom("sona"));
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public static ProducerRecord<String, String> newRecordRandom(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
        Instant now = Instant.now();

        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }

}
