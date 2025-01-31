package net.pincette.mongo.streams;

import static java.util.stream.Collectors.toMap;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.transformFieldNames;
import static net.pincette.json.Transform.transform;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Mapper.map;
import static net.pincette.util.Collections.expand;
import static net.pincette.util.Collections.flatten;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Util.must;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.rs.streams.Message;

/**
 * The <code>$addFields</code> operator.
 *
 * @author Werner Donné
 */
class AddFields {
  private static final String DOT = "_dot_";

  private AddFields() {}

  private static JsonObject addFields(
      final JsonObject json, final Map<String, Function<JsonObject, JsonValue>> functions) {
    final Map<String, JsonValue> newValues = applyFunctions(json, functions);

    return addNewFields(
        transform(
            json,
            new Transformer(
                e -> newValues.containsKey(e.path),
                e -> Optional.of(new JsonEntry(e.path, newValues.remove(e.path))))),
        newValues);
  }

  private static JsonObject addNewFields(
      final JsonObject json, final Map<String, JsonValue> newValues) {
    return unescapeDot(
        from(expand(merge(flatten(escapeDot(json), "."), flatten(newValues, ".")), ".")));
  }

  private static Map<String, JsonValue> applyFunctions(
      final JsonObject json, final Map<String, Function<JsonObject, JsonValue>> functions) {
    return functions.entrySet().stream()
        .collect(toMap(Entry::getKey, e -> e.getValue().apply(json)));
  }

  private static JsonObject escapeDot(final JsonObject json) {
    return transformFieldNames(json, f -> f.replace(".", DOT)).build();
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final Map<String, Function<JsonObject, JsonValue>> functions =
        expression.asJsonObject().entrySet().stream()
            .collect(toMap(Entry::getKey, e -> function(e.getValue(), context.features)));

    return map(m -> m.withValue(addFields(m.value, functions)));
  }

  private static JsonObject unescapeDot(final JsonObject json) {
    return transformFieldNames(json, f -> f.replace(DOT, ".")).build();
  }
}
