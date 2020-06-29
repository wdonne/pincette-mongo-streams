package net.pincette.mongo.streams;

import static java.util.UUID.randomUUID;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.util.Builder.create;
import static net.pincette.util.StreamUtil.iterable;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$unwind</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Unwind {
  private static final String ID = "_id";
  private static final String INCLUDE_ARRAY_INDEX = "includeArrayIndex";
  private static final String NEW_IDS = "newIds";
  private static final String PATH = "path";
  private static final String PRESERVE_NULL_AND_EMPTY_ARRAYS = "preserveNullAndEmptyArrays";

  private Unwind() {}

  private static Stream<JsonObject> emptyArray(
      final JsonObject json, final String includeArrayIndex) {
    return Stream.of(
        create(() -> createObjectBuilder(json))
            .updateIf(b -> includeArrayIndex != null, b -> b.add(includeArrayIndex, NULL))
            .build()
            .build());
  }

  private static boolean hasArray(final JsonObject json, final String path) {
    return getArray(json, toJsonPointer(path)).filter(array -> !array.isEmpty()).isPresent();
  }

  private static Optional<JsonObject> object(final JsonValue value) {
    return Optional.of(value).filter(JsonUtil::isObject).map(JsonValue::asJsonObject);
  }

  private static JsonObjectBuilder set(
      final JsonObject json, final String path, final JsonValue value, final String currentPath) {
    final String prefix = !currentPath.equals("") ? (currentPath + ".") : "";
    final Function<Entry<String, JsonValue>, JsonValue> tryObject =
        e ->
            isObject(e.getValue())
                ? set(e.getValue().asJsonObject(), path, value, prefix + e.getKey()).build()
                : e.getValue();

    return json.entrySet().stream()
        .reduce(
            createObjectBuilder(),
            (b, e) ->
                b.add(e.getKey(), path.equals(prefix + e.getKey()) ? value : tryObject.apply(e)),
            (b1, b2) -> b1);
  }

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression) {
    assert isObject(expression) || isString(expression);

    final String includeArrayIndex =
        object(expression).map(json -> json.getString(INCLUDE_ARRAY_INDEX, null)).orElse(null);
    final boolean newIds =
        object(expression).map(json -> json.getBoolean(NEW_IDS, false)).orElse(false);
    final String path =
        (isString(expression)
                ? asString(expression).getString()
                : expression.asJsonObject().getString(PATH))
            .substring(1);
    final boolean preserveNullAndEmptyArrays =
        object(expression)
            .map(json -> json.getBoolean(PRESERVE_NULL_AND_EMPTY_ARRAYS, false))
            .orElse(false);

    return stream
        .filter((k, v) -> preserveNullAndEmptyArrays || hasArray(v, path))
        .flatMap(
            (k, v) ->
                iterable(
                    unwind(v, path, includeArrayIndex, newIds)
                        .map(
                            unwound ->
                                new KeyValue<>(newIds ? unwound.getString(ID) : k, unwound))));
  }

  private static Stream<JsonObject> unwind(
      final JsonObject json,
      final String path,
      final String includeArrayIndex,
      final boolean newIds) {
    return getArray(json, toJsonPointer(path))
        .filter(array -> !array.isEmpty())
        .map(array -> unwind(json, path, array, includeArrayIndex, newIds))
        .orElseGet(() -> emptyArray(json, includeArrayIndex));
  }

  private static Stream<JsonObject> unwind(
      final JsonObject json,
      final String path,
      final JsonArray array,
      final String includeArrayIndex,
      final boolean newIds) {
    return zip(array.stream(), rangeExclusive(0, array.size()))
        .map(
            pair ->
                create(() -> set(json, path, pair.first, ""))
                    .updateIf(b -> newIds, b -> b.add(ID, createValue(randomUUID().toString())))
                    .updateIf(
                        b -> includeArrayIndex != null, b -> b.add(includeArrayIndex, pair.second))
                    .build()
                    .build());
  }
}
