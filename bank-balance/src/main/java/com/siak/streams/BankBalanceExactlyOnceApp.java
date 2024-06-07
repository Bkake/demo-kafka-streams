package com.siak.streams;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Instant;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;

public class BankBalanceExactlyOnceApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "bank-balance-appV4");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);


        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, JsonNode> bankTransactions = builder.stream(Serdes.String(),
                jsonSerde,"bank-transactions");

        JsonNode initialBalance = initBalance();

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Serdes.String(), jsonSerde)
                .aggregate(
                        () -> initialBalance,
                        (aggKey, newValue, aggValue) -> newBalance(newValue, aggValue),
                        jsonSerde,
                        "bank-balance-agg"
                 );

        bankBalance.to(Serdes.String(), jsonSerde, "bank-balance-exactly-once");
        KafkaStreams streams = new KafkaStreams(builder, props);
            streams.start();
            // shutdown hook to correctly close the streams application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

        long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
    private static JsonNode initBalance() {
        // create the initial json object for balances
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());
        return initialBalance;
    }
}
