package net.pincette.mongo.streams;

import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Optional.empty;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.pull;
import static net.pincette.rs.Util.tap;
import static net.pincette.util.Util.must;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$probe</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Probe {
  private Probe() {}

  private static Processor<Message<String, JsonObject>, Boolean> probe(
      final String name, final String topic, final Context context) {
    final Running running = new Running();

    return pipe(map(
            (Message<String, JsonObject> m) ->
                m.withValue(updateRunning(running, name).orElse(null))))
        .then(filter(m -> m.value != null))
        .then(mapAsync(m -> context.producer.apply(topic, m)));
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    return tap(
        pull(
            probe(
                expression.asJsonObject().getString("name"),
                expression.asJsonObject().getString("topic"),
                context)));
  }

  private static JsonObject toJson(final Running running, final String name) {
    return createObjectBuilder()
        .add("name", name)
        .add("minute", running.minute.toString())
        .add("count", running.count)
        .build();
  }

  private static Optional<JsonObject> updateRunning(final Running running, final String name) {
    final Instant now = now().truncatedTo(MINUTES);

    if (between(running.minute, now).getSeconds() > 59) {
      final JsonObject result = toJson(running, name);

      running.count = 0;
      running.minute = now;

      return Optional.of(result);
    }

    ++running.count;

    return empty();
  }

  private static class Running {
    private long count = 0;
    private Instant minute = now().truncatedTo(MINUTES);
  }
}
