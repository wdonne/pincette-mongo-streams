package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.util.Util.must;

import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$replaceRoot</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class ReplaceRoot {
  private static final String NEW_ROOT = "newRoot";

  private ReplaceRoot() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    must(isObject(expression) && expression.asJsonObject().containsKey(NEW_ROOT));

    return ReplaceWith.stage(stream, expression.asJsonObject().getValue("/" + NEW_ROOT), context);
  }
}
