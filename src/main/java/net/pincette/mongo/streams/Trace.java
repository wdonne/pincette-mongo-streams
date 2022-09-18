package net.pincette.mongo.streams;

import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Util.LOGGER;
import static net.pincette.rs.Mapper.map;

import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.rs.streams.Message;

/**
 * The <code>$trace</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Trace {
  private Trace() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    final Function<JsonObject, JsonValue> function =
        !expression.equals(NULL) ? function(expression, context.features) : null;

    return map(
        m ->
            SideEffect.<Message<String, JsonObject>>run(
                    () ->
                        LOGGER.info(
                            () ->
                                string(function != null ? function.apply(m.value) : m.value, true)))
                .andThenGet(() -> m));
  }
}
