package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static net.pincette.mongo.Expression.function;

import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$replaceRoot</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class ReplaceRoot {
  private static final String ID = "_id";

  private ReplaceRoot() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    final Function<JsonObject, JsonValue> function = function(expression, context.features);

    return stream.map(
        (k, v) ->
            Optional.of(function.apply(v))
                .filter(JsonUtil::isObject)
                .map(JsonValue::asJsonObject)
                .map(
                    result ->
                        new KeyValue<>(ofNullable(result.getString(ID, null)).orElse(k), result))
                .orElseGet(() -> new KeyValue<>(k, v)));
  }
}
