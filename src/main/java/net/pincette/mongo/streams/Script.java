package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static java.util.logging.Level.SEVERE;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.rethrow;
import static net.pincette.util.Util.tryToGet;

import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.rs.streams.Message;

/**
 * @author Werner Donné
 */
class Script {
  private static final String ID = "_id";

  private Script() {}

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

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression,
      final Function<String, UnaryOperator<JsonObject>> transformerObject,
      final Context context) {
    must(isString(expression));

    final UnaryOperator<JsonObject> transformer =
        transformer(expression, transformerObject, context);

    return map(
        m ->
            Optional.of(logCall(transformer, m.value, expression, context))
                .map(
                    result -> message(ofNullable(result.getString(ID, null)).orElse(m.key), result))
                .orElse(m));
  }

  private static UnaryOperator<JsonObject> transformer(
      final JsonValue expression,
      final Function<String, UnaryOperator<JsonObject>> transformerObject,
      final Context context) {
    final String expr = asString(expression).getString();

    return tryToGet(
            () -> transformerObject.apply(expr),
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
