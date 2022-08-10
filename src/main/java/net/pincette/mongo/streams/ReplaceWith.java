package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Util.ID;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.streams.Message.message;

import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.rs.streams.Message;

/**
 * The <code>$replaceWith</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class ReplaceWith {
  private ReplaceWith() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    final Function<JsonObject, JsonValue> function = function(expression, context.features);

    return map(
        m ->
            Optional.of(function.apply(m.value))
                .filter(JsonUtil::isObject)
                .map(JsonValue::asJsonObject)
                .map(
                    result -> message(ofNullable(result.getString(ID, null)).orElse(m.key), result))
                .orElse(m));
  }
}
