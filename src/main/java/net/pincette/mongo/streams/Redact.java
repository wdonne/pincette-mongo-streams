package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$redact</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Redact {
  private static final JsonValue DESCEND = createValue("DESCEND");
  private static final String DESCEND_VAR = "$$DESCEND";
  private static final JsonValue KEEP = createValue("KEEP");
  private static final String KEEP_VAR = "$$KEEP";
  private static final JsonValue PRUNE = createValue("PRUNE");
  private static final String PRUNE_VAR = "$$PRUNE";

  private Redact() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    final Function<JsonObject, JsonValue> function =
        function(
            expression,
            map(pair(DESCEND_VAR, DESCEND), pair(KEEP_VAR, KEEP), pair(PRUNE_VAR, PRUNE)),
            context.features);

    return box(
        map(m -> m.withValue(transform(m.value, function).orElse(null))),
        filter(m -> m.value != null));
  }

  private static Optional<JsonObject> transform(
      final JsonObject json, final Function<JsonObject, JsonValue> function) {
    return Optional.of(function.apply(json))
        .filter(result -> DESCEND.equals(result) || KEEP.equals(result))
        .map(result -> KEEP.equals(result) ? json : transformEmbedded(json, function));
  }

  private static JsonValue transformArray(
      final JsonArray array, final Function<JsonObject, JsonValue> function) {
    return array.stream()
        .map(v -> isObject(v) ? transform(v.asJsonObject(), function).orElse(null) : v)
        .filter(Objects::nonNull)
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  private static JsonObject transformEmbedded(
      final JsonObject json, final Function<JsonObject, JsonValue> function) {
    final BiFunction<JsonObjectBuilder, Entry<String, JsonValue>, JsonObjectBuilder> tryArray =
        (b, e) ->
            b.add(
                e.getKey(),
                isArray(e.getValue())
                    ? transformArray(e.getValue().asJsonArray(), function)
                    : e.getValue());

    return json.entrySet().stream()
        .reduce(
            createObjectBuilder(),
            (b, e) ->
                isObject(e.getValue())
                    ? transform(e.getValue().asJsonObject(), function)
                        .map(j -> b.add(e.getKey(), j))
                        .orElse(b)
                    : tryArray.apply(b, e),
            (b1, b2) -> b1)
        .build();
  }
}
