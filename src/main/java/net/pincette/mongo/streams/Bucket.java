package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.Util.compare;
import static net.pincette.util.Builder.create;
import static net.pincette.util.StreamUtil.slide;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$bucket</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Bucket {
  private static final String AND = "$and";
  private static final String BOUNDARIES = "boundaries";
  private static final String BRANCHES = "branches";
  private static final String CASE = "case";
  private static final String COLLECTION = "_collection";
  private static final String DEFAULT = "default";
  private static final String GTE = "$gte";
  private static final String GROUP_BY = "groupBy";
  private static final String ID = "_id";
  private static final String LT = "$lt";
  private static final String OUTPUT = "output";
  private static final String SWITCH = "$switch";
  private static final String THEN = "then";

  private Bucket() {}

  private static JsonObjectBuilder createCase(
      final JsonValue expression, final JsonValue lower, final JsonValue upper) {
    return createObjectBuilder()
        .add(
            CASE,
            createObjectBuilder()
                .add(
                    AND,
                    createArrayBuilder()
                        .add(
                            createObjectBuilder()
                                .add(GTE, createArrayBuilder().add(expression).add(lower)))
                        .add(
                            createObjectBuilder()
                                .add(LT, createArrayBuilder().add(expression).add(upper)))))
        .add(THEN, lower);
  }

  private static boolean ordered(final JsonArray boundaries) {
    return slide(boundaries.stream(), 2).allMatch(list -> compare(list.get(0), list.get(1)) < 0);
  }

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    assert isObject(expression);

    final JsonObject expr = expression.asJsonObject();
    final JsonArray boundaries = expr.getJsonArray(BOUNDARIES);

    assert boundaries != null && boundaries.size() >= 2 && ordered(boundaries);

    return Group.stage(
        stream, toGroup(expr, boundaries, getValue(expr, "/" + DEFAULT).orElse(null)), context);
  }

  private static JsonObject toGroup(
      final JsonObject expression, final JsonArray boundaries, final JsonValue defaultBucket) {
    return copy(
            expression.getJsonObject(OUTPUT),
            create(JsonUtil::createObjectBuilder)
                .update(
                    b ->
                        b.add(
                            ID,
                            toSwitch(
                                expression.getValue("/" + GROUP_BY), boundaries, defaultBucket)))
                .updateIf(
                    () -> ofNullable(expression.getString(COLLECTION, null)),
                    (b, c) -> b.add(COLLECTION, c))
                .build())
        .build();
  }

  private static JsonObject toSwitch(
      final JsonValue expression, final JsonArray boundaries, final JsonValue defaultBucket) {
    return createObjectBuilder()
        .add(
            SWITCH,
            create(JsonUtil::createObjectBuilder)
                .update(
                    b ->
                        b.add(
                            BRANCHES,
                            slide(boundaries.stream(), 2)
                                .map(list -> createCase(expression, list.get(0), list.get(1)))
                                .reduce(
                                    createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)))
                .updateIf(b -> defaultBucket != null, b -> b.add(DEFAULT, defaultBucket))
                .build())
        .build();
  }
}
