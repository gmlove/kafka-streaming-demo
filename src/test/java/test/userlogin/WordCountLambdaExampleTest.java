package test.userlogin;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCountLambdaExampleTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    @Before
    public void setup() {
        testDriver = new TopologyTestDriver(
                WordCountLambdaExample.createStreamTopo(),
                WordCountLambdaExample.createStreamsConfiguration("localhost:9092"));
        inputTopic = testDriver.createInputTopic(WordCountLambdaExample.inputTopic, stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic(WordCountLambdaExample.outputTopic, stringDeserializer, longDeserializer);
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testOneWord() {
        inputTopic.pipeInput("Hello", Instant.ofEpochMilli(1L));
        final KeyValue<String, Long> output = outputTopic.readKeyValue();
        assertThat(output, equalTo(KeyValue.pair("hello", 1L)));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldCountWords() {
        final List<String> inputValues = Arrays.asList(
                "Hello Kafka Streams",
                "All streams lead to Kafka",
                "Join Kafka Summit",
                "И теперь пошли русские слова"
        );
        final Map<String, Long> expectedWordCounts = new HashMap<>();
        expectedWordCounts.put("hello", 1L);
        expectedWordCounts.put("all", 1L);
        expectedWordCounts.put("streams", 2L);
        expectedWordCounts.put("lead", 1L);
        expectedWordCounts.put("to", 1L);
        expectedWordCounts.put("join", 1L);
        expectedWordCounts.put("kafka", 3L);
        expectedWordCounts.put("summit", 1L);
        expectedWordCounts.put("и", 1L);
        expectedWordCounts.put("теперь", 1L);
        expectedWordCounts.put("пошли", 1L);
        expectedWordCounts.put("русские", 1L);
        expectedWordCounts.put("слова", 1L);

        inputTopic.pipeValueList(inputValues, Instant.ofEpochMilli(1L), Duration.ofMillis(100L));
        assertThat(outputTopic.readKeyValuesToMap(), equalTo(expectedWordCounts));
    }

}