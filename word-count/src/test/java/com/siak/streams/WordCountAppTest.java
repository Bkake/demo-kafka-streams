package com.siak.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class WordCountAppTest  {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private final Serde<String> stringSerde = new Serdes.StringSerde();

    @Before
    public void setUp()  {
        final Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "word-count-app-test");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "dummy:123");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setup test driver
        WordCountApp wordCountApp = new WordCountApp();
        testDriver = new TopologyTestDriver(wordCountApp.createTopology(), props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("word-count-input",
                stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("word-count-output",
                stringSerde.deserializer(), stringSerde.deserializer());
    }

    @After
    public void tearDown()  {
     testDriver.close();
    }

    @Test
    public void makeSureCountAreCorrect() {
        String firstExample = "testing Kafka Streams";
        inputTopic.pipeInput(firstExample);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("testing", "1")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", "1")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("streams", "1")));

        String secondExample = "testing Kafka again";
        inputTopic.pipeInput(secondExample);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("testing", "2")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", "2")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("again", "1")));
    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka Kafka";
        inputTopic.pipeInput(upperCaseString);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", "1")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", "2")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", "3")));
    }

}