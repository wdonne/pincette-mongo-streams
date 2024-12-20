package net.pincette.mongo.streams;

import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Util.generateKey;
import static net.pincette.rs.Mapper.map;

import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$setKey</code> operator.
 *
 * @author Werner Donné
 */
class SetKey {
  private SetKey() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    final Function<JsonObject, JsonValue> key = function(expression, context.features);

    return map(m -> m.withKey(generateKey(key.apply(m.value))));
  }
}
