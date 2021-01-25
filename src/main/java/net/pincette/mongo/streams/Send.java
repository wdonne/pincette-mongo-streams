package net.pincette.mongo.streams;

import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.Expression.function;

import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

class Send {
  private static final String TOPIC = "topic";

  private Send() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    assert isObject(expression);

    final JsonObject expr = expression.asJsonObject();

    assert expr.containsKey(TOPIC);

    final Function<JsonObject, JsonValue> topic =
        function(expr.getValue("/" + TOPIC), context.features);

    return stream
        .map(
            (k, v) ->
                Optional.of(topic.apply(v))
                    .filter(JsonUtil::isString)
                    .map(
                        t ->
                            send(
                                    context.producer,
                                    new ProducerRecord<>(asString(t).getString(), k, v))
                                .thenApply(result -> new KeyValue<>(k, (JsonObject) null))
                                .toCompletableFuture()
                                .join())
                    .orElseGet(() -> new KeyValue<>(k, v)))
        .filter((k, v) -> v != null);
  }
}
