package com.siak.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

public class FavouriteColorApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "favourite-color-v1");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> source = builder.stream("favourite-color-input");

        KStream<String,String> userAndColorStream = source
             .filter((key, value) -> value.contains(","))
             .selectKey((key, value) -> value.split(",")[0].toLowerCase())
             .mapValues(value -> value.split(",")[1].toLowerCase())
             .filter((user, color) -> color.matches("green|blue|red"));

        userAndColorStream.to("user-keys-and-colors");

        KTable<String, String> userAndColorTable = builder.table("user-keys-and-colors");

        KTable<String, String> favouriteColors = userAndColorTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count().mapValues(value -> Long.toString(value));

        favouriteColors.toStream().to("favourite-color-output");

        Topology topology = builder.build();

        try (KafkaStreams streams = new KafkaStreams(topology, props)){
            streams.start();
            // shutdown hook to correctly close the streams application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }
    }
}
