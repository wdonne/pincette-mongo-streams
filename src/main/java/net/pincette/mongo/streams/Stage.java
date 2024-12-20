package net.pincette.mongo.streams;

import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The interface for a pipeline stage.
 *
 * @author Werner Donné
 * @since 3.0
 */
@FunctionalInterface
public interface Stage {
  Processor<Message<String, JsonObject>, Message<String, JsonObject>> apply(
      JsonValue expression, Context context);
}
