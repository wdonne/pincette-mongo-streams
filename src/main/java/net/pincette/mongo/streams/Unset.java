package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isString;

import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$unset</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Unset {
  private Unset() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    assert isArray(expression) || isString(expression);

    return Project.stage(
        stream,
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
