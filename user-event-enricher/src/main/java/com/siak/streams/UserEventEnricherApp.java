package com.siak.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

public class UserEventEnricherApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "user-event-appV4");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        // we get a global table out of Kafka
        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");

        // we get a stream of user purchases
        KStream<String, String> userPurchases = builder.stream("user-purchases");


        // we want to enrich that stream
        KStream<String, String> userPurchasesEnrichedJoin = userPurchases.join(
                usersGlobalTable,
                (key, value) -> key, /* map from the (key, value) of this stream to the key of the GlobalKTable */
                (userPurchase, userInfo) -> "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]"
        );

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(
                usersGlobalTable,
                (key, value) -> key,
                (userPurchase, userInfo) -> {
                    // as this is a left join, userInfo can be null
                    if (userInfo != null) {
                        return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                    } else {
                        return "Purchase=" + userPurchase + ",UserInfo=null";
                    }
                }
        );

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
