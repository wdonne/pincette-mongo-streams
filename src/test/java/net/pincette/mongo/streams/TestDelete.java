package net.pincette.mongo.streams;

import static com.mongodb.client.model.Filters.eq;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Util.must;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.List;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.test.TestRecord;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestDelete extends Base {
  private void delete(final JsonObject message, final JsonValue fields) {
    final MongoCollection<Document> collection =
        resources.database.getCollection("pincette-mongo-streams-test");

    update(collection, message)
        .thenApply(result -> must(result, r -> r))
        .toCompletableFuture()
        .join();

    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(o(f("$delete", o(f("from", v("pincette-mongo-streams-test")), f("on", fields))))),
            list(message));

    assertEquals(1, result.size());
    assertEquals(message, result.get(0).value());
    assertFalse(findOne(collection, eq(ID, "0")).toCompletableFuture().join().isPresent());
  }

  @Test
  @DisplayName("$delete 1")
  public void delete1() {
    delete(o(f(ID, v("0"))), v(ID));
  }

  @Test
  @DisplayName("$delete 2")
  public void delete2() {
    delete(o(f(ID, v("0")), f("test", v(0))), a(v(ID), v("test")));
  }
}
