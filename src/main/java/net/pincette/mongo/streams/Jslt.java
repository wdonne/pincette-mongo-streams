package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static java.util.logging.Level.SEVERE;
import static net.pincette.json.Jslt.transformerObject;
import static net.pincette.json.Jslt.tryReader;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.rethrow;
import static net.pincette.util.Util.tryToGet;

import java.util.Optional;
import java.util.function.UnaryOperator;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
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

  private static JsonObject logCall(
      final UnaryOperator<JsonObject> transformer,
      final JsonObject json,
      final JsonValue expression,
      final Context context) {
    return tryToGet(
            () -> transformer.apply(json),
            e ->
                SideEffect.<JsonObject>run(
                        () -> {
                          if (context.logger != null) {
                            context.logger.log(
                                SEVERE,
                                e,
                                () ->
                                    "Expression:\n"
                                        + asString(expression)
                                        + "\n\nWith:\n"
                                        + string(json));
                          }

                          rethrow(e);
                        })
                    .andThenGet(() -> null))
        .orElse(null);
  }

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    must(isString(expression));

    final UnaryOperator<JsonObject> transformer = transformer(expression, context);

    return stream.map(
        (k, v) ->
            Optional.of(logCall(transformer, v, expression, context))
                .map(
                    result ->
                        new KeyValue<>(ofNullable(result.getString(ID, null)).orElse(k), result))
                .orElseGet(() -> new KeyValue<>(k, v)));
  }

  private static UnaryOperator<JsonObject> transformer(
      final JsonValue expression, final Context context) {
    final String expr = asString(expression).getString();
    final net.pincette.json.Jslt.Context transformerContext =
        new net.pincette.json.Jslt.Context(tryReader(expr));

    return tryToGet(
            () ->
                transformerObject(
                    context.features != null
                        ? transformerContext
                            .withFunctions(context.features.customJsltFunctions)
                            .withResolver(context.features.jsltResolver)
                        : transformerContext),
            e ->
                SideEffect.<UnaryOperator<JsonObject>>run(
                        () -> {
                          if (context.logger != null) {
                            context.logger.log(SEVERE, e, () -> "Expression:\n" + expr);
                          }

                          rethrow(e);
                        })
                    .andThenGet(() -> null))
        .orElse(null);
  }
}
