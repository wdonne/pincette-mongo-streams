package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.Match.predicate;
import static net.pincette.rs.Filter.filter;
import static net.pincette.util.Util.must;

import java.util.concurrent.Flow.Processor;
import java.util.function.Predicate;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$match</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Match {
  private Match() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final Predicate<JsonObject> predicate = predicate(expression.asJsonObject(), context.features);

    return filter(m -> predicate.test(m.value));
  }
}
