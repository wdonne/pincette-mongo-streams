package net.pincette.mongo.streams;

import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isObject;

import java.time.Instant;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$probe</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Probe {
  private Probe() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression) {
    assert isObject(expression);

    final String name = expression.asJsonObject().getString("name");
    final Running running = new Running();
    final String topic = expression.asJsonObject().getString("topic");

    stream.mapValues(v -> updateRunning(running, name)).filter((k, v) -> v != null).to(topic);

    return stream;
  }

  private static JsonObject toJson(final Running running, final String name) {
    return createObjectBuilder()
        .add("name", name)
        .add("minute", running.minute.toString())
        .add("count", running.count)
        .build();
  }

  private static JsonObject updateRunning(final Running running, final String name) {
    final Instant now = now().truncatedTo(MINUTES);

    if (between(running.minute, now).getSeconds() > 59) {
      final JsonObject result = toJson(running, name);

      running.count = 0;
      running.minute = now;

      return result;
    }

    ++running.count;

    return null;
  }

  private static class Running {
    private long count = 0;
    private Instant minute = now().truncatedTo(MINUTES);
  }
}
