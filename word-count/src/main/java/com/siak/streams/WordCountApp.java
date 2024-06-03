package com.siak.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

public class WordCountApp {
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "word-count-app-v2");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(STATE_DIR_CONFIG, String.format("%s%s", "word-count-app", UUID.randomUUID()));


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        KTable<String, String> wordCounts = wordCountInput
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .selectKey((ignoredKey, value) -> value)
                .groupByKey()
                .count().mapValues(value -> Long.toString(value));

        wordCounts.toStream().to("word-count-output",
                Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();

        try (KafkaStreams streams = new KafkaStreams(topology, props)){
            streams.start();
            // shutdown hook to correctly close the streams application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }

    }
}
