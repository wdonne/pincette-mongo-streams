package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.util.Util.must;

import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$replaceRoot</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class ReplaceRoot {
  private static final String NEW_ROOT = "newRoot";

  private ReplaceRoot() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression) && expression.asJsonObject().containsKey(NEW_ROOT));

    return ReplaceWith.stage(expression.asJsonObject().getValue("/" + NEW_ROOT), context);
  }
}
