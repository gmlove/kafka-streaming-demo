package test.userlogin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;

public class TotalLoginUsers {

    private static final String APP_ID = "LOGIN_USERS_STAT";
    private static final Object CLIENT_ID = APP_ID + "_CLIENT";
    private DateService dateService;
    private String inputTopic;
    private String outputTopic;
    private final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.getDefault());

    public Topology createStreamTopo() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(inputTopic);

        final KStream<String, String> loginUserIdsInToday = textLines
                .filter((key, log) -> UserLogin.isInRequiredFormat(log))
                .filter((key, log) -> UserLogin.isFromDate(log, dateFormat.format(dateService.today())));
        final KTable<String, Long> totalUserLoginCount = loginUserIdsInToday
                .groupBy((key, log) -> "total_user_login/" + UserLogin.loginDate(log))
                .count();
        final KTable<String, Long> uniqueLoginUsers = loginUserIdsInToday
                .groupBy((key, log) -> "user_login/" + UserLogin.userId(log) + "/" + UserLogin.loginDate(log))
                .count()
                .toStream()
                .filter((key, count) -> count == 1)
                .groupBy((key, count) -> "unique_user_login/" + key.split("/")[2])
                .count();
        totalUserLoginCount.toStream()
                // We have to convert the result values to string in order for kafka-console-consumer to display the result correctly, since it just reads the value as string
                .map((key, value) -> new KeyValue<>("", String.format("key=%s, value=%s", key, value)))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        uniqueLoginUsers.toStream()
                .map((key, value) -> new KeyValue<>("", String.format("key=%s, value=%s", key, value)))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public Properties createStreamsConfiguration(String bootstrapServers) {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/" + APP_ID);
        return streamsConfiguration;
    }

    interface DateService {
        LocalDate today();
    }

    public TotalLoginUsers(DateService dateService, String inputTopic, String outputTopic) {
        this.dateService = dateService;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Message {
        public final String payload;
        @JsonCreator
        public Message(@JsonProperty("payload") String payload) {
            this.payload = payload;
        }
    }

    public static class UserLogin {
        static ObjectMapper objectMapper = new ObjectMapper();

        public static String payload(String log) {
            try {
                return objectMapper.readValue(log, Message.class).payload;
            } catch (JsonProcessingException e) {
                System.err.println("unrecognized input: " + log);
                throw new RuntimeException(e);
            }
        }

        public static boolean isInRequiredFormat(String log) {
            try {
                payload(log);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        public static String userId(String log) {
            return payload(log).split(",")[0].trim();
        }

        public static String loginAt(String log) {
            return payload(log).split(",")[1].trim();
        }

        public static boolean isFromDate(String log, String dateStr) {
            System.out.println("got data: " + log + ", for date=" + dateStr);
            return loginAt(log).startsWith(dateStr);
        }

        public static String loginDate(String log) {
            return loginAt(log).substring(0, "yyyy-MM-dd".length());
        }
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            System.err.println("Parameters required: bootstrapServers inputTopicName outputTopicName");
            System.exit(1);
        }
        final String bootstrapServers = args[0];
        final String inputTopic = args[1];
        final String outputTopic = args[2];

        System.out.println(String.format("bootstrapServers=%s, inputTopic=%s, outputTopic=%s", bootstrapServers, inputTopic, outputTopic));

        final TotalLoginUsers totalLoginUsers = new TotalLoginUsers(() -> LocalDate.now(), inputTopic, outputTopic);

        final KafkaStreams streams = new KafkaStreams(
                totalLoginUsers.createStreamTopo(),
                totalLoginUsers.createStreamsConfiguration(bootstrapServers));

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
