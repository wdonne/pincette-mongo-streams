package net.pincette.mongo.streams;

import static java.time.Duration.ofMillis;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.must;

import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.rs.streams.Message;

class Delay {
  private static final String DURATION = "duration";
  private static final String TOPIC = "topic";

  private Delay() {}

  private static void sendAfter(
      final Message<String, JsonObject> message,
      final long duration,
      final String topic,
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> next,
      final Context context) {
    composeAsyncAfter(() -> context.producer.apply(topic, message), ofMillis(duration))
        .thenAccept(result -> must(result, r -> r))
        .exceptionally(
            t -> {
              next.onError(t);
              return null;
            });
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();
    final Function<JsonObject, JsonValue> duration =
        function(expr.getValue("/" + DURATION), context.features);
    final Processor<Message<String, JsonObject>, Message<String, JsonObject>> filter =
        filter(m -> m.value != null);
    final Function<JsonObject, JsonValue> topic =
        function(expr.getValue("/" + TOPIC), context.features);

    return box(
        map(
            m ->
                Optional.of(pair(duration.apply(m.value), topic.apply(m.value)))
                    .filter(pair -> isLong(pair.first) && isString(pair.second))
                    .map(
                        pair ->
                            SideEffect.<Message<String, JsonObject>>run(
                                    () ->
                                        sendAfter(
                                            m,
                                            asLong(pair.first),
                                            asString(pair.second).getString(),
                                            filter,
                                            context))
                                .andThenGet(() -> m.withValue(null)))
                    .orElse(m)),
        filter);
  }
}
