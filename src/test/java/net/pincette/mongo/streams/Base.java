package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static net.pincette.mongo.streams.Pipeline.create;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Reducer.forEach;
import static net.pincette.rs.Reducer.forEachJoin;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

import java.util.List;
import java.util.Properties;
import javax.json.JsonArray;
import javax.json.JsonObject;
import net.pincette.jes.util.JsonDeserializer;
import net.pincette.jes.util.JsonSerde;
import net.pincette.jes.util.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class Base {
  static final String APP = "pincette-mongo-streams";
  static final String ID = "_id";
  private static final Properties CONFIG = config();
  protected static Resources resources;

  @AfterAll
  public static void after() {
    cleanUpCollections();
    resources.close();
  }

  @BeforeAll
  public static void before() {
    resources = new Resources();
    cleanUpCollections();
  }

  private static void cleanUpCollections() {
    forEachJoin(
        with(resources.database.listCollectionNames()).filter(name -> name.startsWith(APP)).get(),
        name -> forEach(resources.database.getCollection(name).drop(), v -> {}));
  }

  private static Properties config() {
    final Properties properties = new Properties();

    properties.put(APPLICATION_ID_CONFIG, "test");
    properties.put(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

    return properties;
  }

  private static List<TestRecord<String, JsonObject>> inputMessages(
      final List<JsonObject> messages) {
    return messages.stream()
        .map(
            m ->
                new TestRecord<>(
                    ofNullable(m.getString(ID, null)).orElseGet(() -> randomUUID().toString()), m))
        .collect(toList());
  }

  protected void drop(final String collection) {
    forEachJoin(resources.database.getCollection(collection).drop(), v -> {});
  }

  protected List<TestRecord<String, JsonObject>> runTest(
      final JsonArray pipeline, final List<JsonObject> messages) {
    final StreamsBuilder builder = new StreamsBuilder();

    create(APP, builder.stream("input-topic"), pipeline, resources.database).to("output-topic");

    return tryToGetWithRethrow(
            () -> new TopologyTestDriver(builder.build(), CONFIG),
            testDriver -> {
              final TestInputTopic<String, JsonObject> inputTopic =
                  testDriver.createInputTopic(
                      "input-topic", new StringSerializer(), new JsonSerializer());
              final TestOutputTopic<String, JsonObject> outputTopic =
                  testDriver.createOutputTopic(
                      "output-topic", new StringDeserializer(), new JsonDeserializer());

              inputTopic.pipeRecordList(inputMessages(messages));

              return outputTopic.readRecordsToList();
            })
        .orElse(null);
  }
}
