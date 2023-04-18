package net.pincette.mongo.streams;

import static java.time.Duration.ofMillis;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.util.ScheduledCompletionStage.supplyAsyncAfter;
import static net.pincette.util.Util.must;

import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$wait</code> operator.
 *
 * @author Werner Donné
 */
class Wait {
  private Wait() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression) {
    must(isLong(expression));

    final long wait = asLong(expression);

    return mapAsyncSequential(m -> supplyAsyncAfter(() -> m, ofMillis(wait)));
  }
}
