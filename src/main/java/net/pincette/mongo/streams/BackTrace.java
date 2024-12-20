package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.streams.Util.LOGGER;
import static net.pincette.rs.Probe.probe;
import static net.pincette.util.Util.must;

import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$backTrace</code> operator.
 *
 * @author Werner Donné
 */
class BackTrace {
  private static final String NAME = "name";

  private BackTrace() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression) {
    must(isObject(expression));

    final String name = expression.asJsonObject().getString(NAME, null);

    return probe(
        requested ->
            LOGGER.info(
                () ->
                    "Backpressure request "
                        + (name != null ? ("for " + name + " ") : "")
                        + "of "
                        + requested));
  }
}
