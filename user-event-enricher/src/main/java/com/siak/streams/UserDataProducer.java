package com.siak.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class UserDataProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDataProducer.class);
    public static void main(String[] args) throws Exception {
        Properties config = new Properties();
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ACKS_CONFIG, "all");
        config.put(RETRIES_CONFIG, "3");
        config.put(LINGER_MS_CONFIG, "1");
        config.put(ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (Producer<String,String> producer = new KafkaProducer<>(config)){

            // 1 - we create a new user, then we send some data to Kafka
            LOGGER.info("\nExample 1 - new user\n");
            producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
            producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

            Thread.sleep(10000);

            // 2 - we receive user purchase, but it doesn't exist in Kafka
            LOGGER.info("\nExample 2 - non existing user\n");
            producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();

            Thread.sleep(10000);

            // 3 - we update user "john", and send a new transaction
            LOGGER.info("\nExample 3 - update to user\n");
            producer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
            producer.send(purchaseRecord("john", "Oranges (3)")).get();

            Thread.sleep(10000);

            // 4 - we send a user purchase for stephane, but it exists in Kafka later
            LOGGER.info("\nExample 4 - non existing user then user\n");
            producer.send(purchaseRecord("bkake", "Computer (4)")).get();
            producer.send(userRecord("bkake", "First=bkake,Last=Bangaly,GitHub=bkake")).get();
            producer.send(purchaseRecord("bkake", "Books (4)")).get();
            producer.send(userRecord("bkake", null)).get(); // delete for cleanup

            Thread.sleep(10000);

            // 5 - we create a user, but it gets deleted before any purchase comes through
            LOGGER.info("\nExample 5 - user then delete then data\n");
            producer.send(userRecord("alice", "First=Alice")).get();
            producer.send(userRecord("alice", null)).get(); // that's the delete record
            producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();

            Thread.sleep(10000);

            LOGGER.info("End of demo");
        }
    }
    private static ProducerRecord<String, String> userRecord(String key, String value){
        return new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchases", key, value);
    }
}
