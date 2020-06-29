package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.Expression.function;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$addFields</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class AddFields {
  private AddFields() {}

  private static JsonObjectBuilder addFields(
      final String path,
      final JsonObject json,
      final JsonObject evaluated,
      final Map<String, Map<String, Function<JsonObject, JsonValue>>> newFields) {
    final String prefix = !path.equals("") ? (path + ".") : "";
    final JsonObjectBuilder builder =
        json.entrySet().stream()
            .reduce(
                createObjectBuilder(),
                (b, e) ->
                    isObject(e.getValue())
                        ? b.add(
                            e.getKey(),
                            addFields(
                                prefix + e.getKey(),
                                e.getValue().asJsonObject(),
                                evaluated,
                                newFields))
                        : b.add(e.getKey(), e.getValue()),
                (b1, b2) -> b1);

    return ofNullable(newFields.get(path))
        .map(
            fields ->
                fields.entrySet().stream()
                    .reduce(
                        builder,
                        (b, e) -> b.add(e.getKey(), e.getValue().apply(evaluated)),
                        (b1, b2) -> b1))
        .orElse(builder);
  }

  private static Map<String, Map<String, Function<JsonObject, JsonValue>>> grouped(
      final Map<String, Function<JsonObject, JsonValue>> newFields) {
    return newFields.entrySet().stream()
        .collect(
            groupingBy(
                e ->
                    Optional.of(e.getKey().lastIndexOf('.'))
                        .filter(index -> index != -1)
                        .map(index -> e.getKey().substring(0, index))
                        .orElse(""),
                toMap(
                    e ->
                        Optional.of(e.getKey().lastIndexOf('.'))
                            .filter(index -> index != -1)
                            .map(index -> e.getKey().substring(index + 1))
                            .orElseGet(e::getKey),
                    Entry::getValue)));
  }

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression) {
    assert isObject(expression);

    final Map<String, Map<String, Function<JsonObject, JsonValue>>> grouped =
        grouped(
            expression.asJsonObject().entrySet().stream()
                .collect(toMap(Entry::getKey, e -> function(e.getValue()))));

    return stream.mapValues(v -> addFields("", v, v, grouped).build());
  }
}
