package net.pincette.mongo.streams;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;

/**
 * Some utilities.
 *
 * @author Werner Donn\u00e9
 */
class Util {
  private static final String AND = "$and";
  private static final String EQ = "$eq";
  private static final String ON = "on";

  private Util() {}

  private static JsonObject addMatchQueries(final List<Pair<String, JsonValue>> queries) {
    return createObjectBuilder()
        .add(
            AND,
            queries.stream()
                .reduce(
                    createArrayBuilder(), (b, p) -> b.add(eq(p.first, p.second)), (b1, b2) -> b1))
        .build();
  }

  private static JsonObjectBuilder eq(final String field, final JsonValue value) {
    return createObjectBuilder().add(field, createObjectBuilder().add(EQ, value));
  }

  static String generateKey(final JsonValue key) {
    return toNative(key).toString();
  }

  private static List<Pair<String, JsonValue>> getValues(
      final JsonObject json, final Set<String> fields) {
    return fields.stream()
        .map(field -> getValue(json, toJsonPointer(field)).map(value -> pair(field, value)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toList());
  }

  static Set<String> matchFields(final JsonObject expression, final String defaultField) {
    return getValue(expression, "/" + ON)
        .map(
            value ->
                isArray(value)
                    ? value.asJsonArray().stream()
                        .filter(JsonUtil::isString)
                        .map(JsonUtil::asString)
                        .map(JsonString::getString)
                        .collect(toSet())
                    : set(asString(value).getString()))
        .orElseGet(() -> defaultField != null ? set(defaultField) : emptySet());
  }

  static Optional<JsonObject> matchQuery(final JsonObject json, final Set<String> fields) {
    return fields.size() == 1
        ? matchQuerySingle(json, fields.iterator().next())
        : matchQueryMultiple(json, fields);
  }

  private static Optional<JsonObject> matchQueryMultiple(
      final JsonObject json, final Set<String> fields) {
    return Optional.of(getValues(json, fields))
        .filter(pairs -> pairs.size() == fields.size())
        .map(Util::addMatchQueries);
  }

  private static Optional<JsonObject> matchQuerySingle(final JsonObject json, final String field) {
    return getValue(json, toJsonPointer(field)).map(value -> eq(field, value).build());
  }
}
