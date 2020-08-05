package test.userlogin;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

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

import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;

public class TotalLoginUsersTest {

    private TotalLoginUsers totalLoginUsers;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    public static final LocalDate TODAY = LocalDate.of(2020, 8, 1);

    private String inputTopicName = "userlogin-input";
    private String outputTopicName = "userlogin-output";

    @Before
    public void setup() {
        totalLoginUsers = new TotalLoginUsers(() -> TODAY, inputTopicName, outputTopicName);
        testDriver = new TopologyTestDriver(
                totalLoginUsers.createStreamTopo(),
                totalLoginUsers.createStreamsConfiguration("localhost:9092"));
        inputTopic = testDriver.createInputTopic(inputTopicName, stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic(outputTopicName, stringDeserializer, stringDeserializer);
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
    public void should_calculate_login_users_for_today() {
        inputTopic.pipeInput(input("user1, 2020-08-01 12:00:00"), Instant.now());
        inputTopic.pipeInput(input("user1, 2020-07-31 12:00:00"), Instant.now());
        inputTopic.pipeInput(input("user1, 2020-08-01 12:01:00"), Instant.now());
        inputTopic.pipeInput(input("user1, 2020-08-02 12:00:00"), Instant.now());
        inputTopic.pipeInput(input("user2, 2020-08-01 12:01:00"), Instant.now());
        assertThat(outputTopic.readKeyValuesToList(), equalTo(Arrays.asList(
                output("total_user_login/2020-08-01", 1L),
                output("unique_user_login/2020-08-01", 1L),
                output("total_user_login/2020-08-01", 2L),
                output("total_user_login/2020-08-01", 3L),
                output("unique_user_login/2020-08-01", 2L)
        )));
    }

    private KeyValue<String, String> output(String key, long value) {
        return new KeyValue<>("", String.format("key=%s, value=%s", key, value));
    }

    private String input(String input) {
        return "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"" + input + "\"}";
    }

}