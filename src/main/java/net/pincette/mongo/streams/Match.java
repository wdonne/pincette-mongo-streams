package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.Match.predicate;

import java.util.function.Predicate;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$match</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Match {
  private Match() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression) {
    assert isObject(expression);

    final Predicate<JsonObject> predicate = predicate(expression.asJsonObject());

    return stream.filter((k, v) -> predicate.test(v));
  }
}
