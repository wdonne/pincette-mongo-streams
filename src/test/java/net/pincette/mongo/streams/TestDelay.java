package net.pincette.mongo.streams;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.util.Collections.list;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.json.JsonObject;
import net.pincette.rs.streams.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestDelay extends Base {
  @Test
  @DisplayName("$delay")
  void delay() {
    final CompletableFuture<Boolean> future = new CompletableFuture<>();
    final JsonObject message = o(f("test", v(0)));
    final List<JsonObject> sent = new ArrayList<>();
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$delay", o(f("duration", v(5000)), f("topic", v("test")))))),
            list(message),
            (topic, msg) -> {
              sent.add(msg.value);
              future.complete(true);

              return completedFuture(true);
            });

    future.join();
    assertEquals(0, result.size());
    assertEquals(1, sent.size());
    assertEquals(message, sent.get(0));
  }
}
