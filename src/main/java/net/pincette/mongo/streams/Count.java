package net.pincette.mongo.streams;

import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.util.Util.must;

import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$count</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Count {
  private static final String ID = "_id";
  private static final String SUM = "$sum";

  private Count() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    must(isString(expression));

    final String field = asString(expression).getString();

    return Group.stage(
            stream,
            createObjectBuilder()
                .add(ID, NULL)
                .add(field, createObjectBuilder().add(SUM, 1))
                .build(),
            context)
        .mapValues(v -> createObjectBuilder(v).remove(ID).build());
  }
}
