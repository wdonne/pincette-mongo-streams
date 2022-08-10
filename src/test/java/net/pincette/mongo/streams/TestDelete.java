package net.pincette.mongo.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.util.Collections.list;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.must;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestDelete extends Base {
  private static CompletionStage<Boolean> waitUntilGone(
      final Supplier<CompletionStage<Boolean>> fn) {
    return composeAsyncAfter(fn, ofMillis(100))
        .thenComposeAsync(result -> !result ? completedFuture(false) : waitUntilGone(fn));
  }

  private void delete(final JsonObject message, final JsonValue fields) {
    final MongoCollection<Document> collection =
        resources.database.getCollection("pincette-mongo-streams-test");

    update(collection, message)
        .thenApply(result -> must(result, r -> r))
        .toCompletableFuture()
        .join();

    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$delete", o(f("from", v("pincette-mongo-streams-test")), f("on", fields))))),
            list(message));

    assertEquals(1, result.size());
    assertEquals(message, result.get(0).value);
    assertFalse(
        waitUntilGone(() -> findOne(collection, eq(ID, "0")).thenApply(Optional::isPresent))
            .toCompletableFuture()
            .join());
  }

  @Test
  @DisplayName("$delete 1")
  void delete1() {
    delete(o(f(ID, v("0"))), v(ID));
  }

  @Test
  @DisplayName("$delete 2")
  void delete2() {
    delete(o(f(ID, v("0")), f("test", v(0))), a(v(ID), v("test")));
  }
}
