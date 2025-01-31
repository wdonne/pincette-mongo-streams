package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
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
 * @author Werner Donné
 */
class Trace {
  private Trace() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    final Function<JsonObject, JsonValue> function =
        !expression.equals(NULL) ? function(expression, context.features) : null;
    final var logger = ofNullable(context.logger).orElse(LOGGER);

    return map(
        m ->
            SideEffect.<Message<String, JsonObject>>run(
                    () ->
                        logger.info(
                            () ->
                                string(
                                    function != null ? function.apply(m.value) : m.value, false)))
                .andThenGet(() -> m));
  }
}
