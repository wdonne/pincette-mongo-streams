package net.pincette.mongo.streams;

import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Util.generateKey;

import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$setKey</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class SetKey {
  private SetKey() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression) {
    final Function<JsonObject, JsonValue> key = function(expression);

    return stream.map((k, v) -> new KeyValue<>(generateKey(key.apply(v)), v));
  }
}
