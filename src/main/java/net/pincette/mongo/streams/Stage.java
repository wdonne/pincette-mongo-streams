package net.pincette.mongo.streams;

import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * An implementation should return another stream.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
@FunctionalInterface
interface Stage {
  KStream<String, JsonObject> apply(
      KStream<String, JsonObject> stream, JsonValue expression, Context context);
}
