package net.pincette.mongo.streams;

import static com.mongodb.client.model.Filters.eq;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.util.Collections.list;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestOut extends Base {
  @Test
  @DisplayName("$out")
  void out() {
    final JsonObject message = o(f(ID, v("0")));
    final List<TestRecord<String, JsonObject>> result =
        runTest(a(o(f("$out", v("pincette-mongo-streams-test")))), list(message));

    assertEquals(1, result.size());
    assertEquals(message, result.get(0).value());
    assertEquals(
        message,
        findOne(resources.database.getCollection("pincette-mongo-streams-test"), eq(ID, "0"))
            .toCompletableFuture()
            .join()
            .orElseGet(JsonUtil::emptyObject));
  }
}
