package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.util.Util.must;

import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$out</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Out {
  private static final String ID = "_id";
  private static final String INSERT = "insert";
  private static final String INTO = "into";
  private static final String ON = "on";
  private static final String REPLACE = "replace";
  private static final String WHEN_MATCHED = "whenMatched";
  private static final String WHEN_NOT_MATCHED = "whenNotMatched";

  private Out() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    must(isString(expression));

    return Merge.stage(
        stream,
        createObjectBuilder()
            .add(INTO, asString(expression).getString())
            .add(ON, ID)
            .add(WHEN_MATCHED, REPLACE)
            .add(WHEN_NOT_MATCHED, INSERT)
            .build(),
        context);
  }
}
