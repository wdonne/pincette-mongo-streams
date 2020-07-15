package net.pincette.mongo.streams;

import static java.util.logging.Logger.getLogger;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.Expression.function;

import java.util.function.Function;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$trace</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Trace {
  private static final String LOGGER = "net.pincette.mongo.streams";

  private Trace() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    final Function<JsonObject, JsonValue> function =
        !expression.equals(NULL) ? function(expression, context.features) : null;
    final Logger logger = getLogger(LOGGER);

    return stream.mapValues(
        v ->
            SideEffect.<JsonObject>run(
                    () -> logger.info(() -> string(function != null ? function.apply(v) : v, true)))
                .andThenGet(() -> v));
  }
}
