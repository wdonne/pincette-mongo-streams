package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static net.pincette.json.Jslt.transformerObject;
import static net.pincette.json.Jslt.tryReader;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isString;

import java.util.Optional;
import java.util.function.UnaryOperator;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The <code>$jslt</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Jslt {
  private static final String ID = "_id";

  private Jslt() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    assert isString(expression);

    final UnaryOperator<JsonObject> transformer = transformer(expression, context);

    return stream.map(
        (k, v) ->
            Optional.of(transformer.apply(v))
                .map(
                    result ->
                        new KeyValue<>(ofNullable(result.getString(ID, null)).orElse(k), result))
                .orElseGet(() -> new KeyValue<>(k, v)));
  }

  private static UnaryOperator<JsonObject> transformer(
      final JsonValue expression, final Context context) {
    final net.pincette.json.Jslt.Context transformerContext =
        new net.pincette.json.Jslt.Context(tryReader(asString(expression).getString()));

    return transformerObject(
        context.features != null
            ? transformerContext
                .withFunctions(context.features.customJsltFunctions)
                .withResolver(context.features.jsltResolver)
            : transformerContext);
  }
}
