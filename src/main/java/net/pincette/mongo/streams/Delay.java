package net.pincette.mongo.streams;

import static java.time.Duration.ofMillis;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Util.RETRY;
import static net.pincette.mongo.streams.Util.exceptionLogger;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToGetForever;

import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

class Delay {
  private static final String DURATION = "duration";
  private static final String TOPIC = "topic";

  private Delay() {}

  private static void sendAfter(
      final String key,
      final JsonObject value,
      final long duration,
      final String topic,
      final Context context) {
    composeAsyncAfter(
            () ->
                tryToGetForever(
                    () -> send(context.producer, new ProducerRecord<>(topic, key, value)),
                    RETRY,
                    e -> exceptionLogger(e, "$delay", context)),
            ofMillis(duration))
        .thenApply(result -> must(result, r -> r));
  }

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();
    final Function<JsonObject, JsonValue> duration =
        function(expr.getValue("/" + DURATION), context.features);
    final Function<JsonObject, JsonValue> topic =
        function(expr.getValue("/" + TOPIC), context.features);

    return stream
        .map(
            (k, v) ->
                Optional.of(pair(duration.apply(v), topic.apply(v)))
                    .filter(pair -> isLong(pair.first) && isString(pair.second))
                    .map(
                        pair ->
                            SideEffect.<KeyValue<String, JsonObject>>run(
                                    () ->
                                        sendAfter(
                                            k,
                                            v,
                                            asLong(pair.first),
                                            asString(pair.second).getString(),
                                            context))
                                .andThenGet(() -> new KeyValue<>(k, null)))
                    .orElseGet(() -> new KeyValue<>(k, v)))
        .filter((k, v) -> v != null);
  }
}
