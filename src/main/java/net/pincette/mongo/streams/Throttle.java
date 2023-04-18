package net.pincette.mongo.streams;

import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.util.ScheduledCompletionStage.supplyAsyncAfter;
import static net.pincette.util.Util.must;

import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$throttle</code> operator.
 *
 * @author Werner Donné
 * @since 3.0
 */
class Throttle {
  private static final String MAX_PER_SECOND = "maxPerSecond";

  private Throttle() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression) {
    must(isObject(expression));

    final Running running = new Running(expression.asJsonObject().getInt(MAX_PER_SECOND));

    return mapAsyncSequential(m -> updateRunning(running).thenApply(r -> m));
  }

  private static CompletionStage<Boolean> updateRunning(final Running running) {
    final Instant now = now().truncatedTo(SECONDS);

    if (between(running.second, now).toMillis() > 999) {
      running.count = 0;
      running.second = now;
    }

    return ++running.count >= running.maxPerSecond
        ? supplyAsyncAfter(() -> true, between(now(), now.plusSeconds(1)))
        : completedFuture(true);
  }

  private static class Running {
    private final int maxPerSecond;
    private long count = 0;
    private Instant second = now().truncatedTo(SECONDS);

    private Running(final int maxPerSecond) {
      this.maxPerSecond = maxPerSecond;
    }
  }
}
