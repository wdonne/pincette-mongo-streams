package net.pincette.mongo.streams;

import static net.pincette.json.Jq.transformerObject;
import static net.pincette.json.Jq.tryReader;

import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$jq</code> operator.
 *
 * @author Werner Donné
 */
class Jq {
  private Jq() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    return Script.stage(
        expression,
        expr -> {
          final net.pincette.json.Jq.Context transformerContext =
              new net.pincette.json.Jq.Context(tryReader(expr));

          return transformerObject(
              context.features != null
                  ? transformerContext.withModuleLoader(context.features.jqModuleLoader)
                  : transformerContext);
        },
        context);
  }
}
