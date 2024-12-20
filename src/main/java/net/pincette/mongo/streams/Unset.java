package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.util.Util.must;

import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.rs.streams.Message;

/**
 * The <code>$unset</code> operator.
 *
 * @author Werner Donné
 */
class Unset {
  private Unset() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isArray(expression) || isString(expression));

    return Project.stage(
        isArray(expression)
            ? expression.asJsonArray().stream()
                .filter(JsonUtil::isString)
                .map(JsonUtil::asString)
                .map(JsonString::getString)
                .reduce(createObjectBuilder(), (b, f) -> b.add(f, 0), (b1, b2) -> b1)
                .build()
            : createObjectBuilder().add(asString(expression).getString(), 0).build(),
        context);
  }
}
