package net.pincette.mongo.streams;

import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The interface for a pipeline stage. An implementation should return another stream.
 *
 * @author Werner Donn\u00e9
 * @since 1.1
 */
@FunctionalInterface
public interface Stage {
  KStream<String, JsonObject> apply(
      KStream<String, JsonObject> stream, JsonValue expression, Context context);
}
