package net.pincette.mongo.streams;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Pipeline.SEND;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.onCancelProcessor;
import static net.pincette.rs.Util.onCompleteProcessor;
import static net.pincette.util.Util.must;

import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.rs.streams.Message;
import net.pincette.util.State;

/**
 * The $send operator.
 *
 * @author Werner Donné
 */
class Send {
  private static final String TOPIC = "topic";

  private Send() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();

    must(expr.containsKey(TOPIC));

    final State<Boolean> stop = new State<>(false);
    final Function<JsonObject, JsonValue> topic =
        function(expr.getValue("/" + TOPIC), context.features);

    return pipe(mapAsyncSequential(
            (Message<String, JsonObject> m) ->
                Optional.of(topic.apply(m.value))
                    .filter(JsonUtil::isString)
                    .map(
                        t ->
                            tryForever(
                                () ->
                                    context
                                        .producer
                                        .apply(asString(t).getString(), m)
                                        .thenApply(result -> m.withValue(null)),
                                SEND,
                                stop::get,
                                () -> "Topic " + t + ", send: " + string(m.value),
                                context))
                    .orElseGet(() -> completedFuture(m))))
        .then(filter(m -> m.value != null))
        .then(onCancelProcessor(() -> stop.set(true)))
        .then(onCompleteProcessor(() -> stop.set(true)));
  }
}
