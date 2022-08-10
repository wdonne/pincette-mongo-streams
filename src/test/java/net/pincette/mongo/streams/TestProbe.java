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
import javax.json.JsonObject;
import net.pincette.rs.streams.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestProbe extends Base {
  @Test
  @DisplayName("$probe")
  void probe() {
    final JsonObject message = o(f("test", v(0)));
    final List<String> names = new ArrayList<>();
    final List<Message<String, JsonObject>> result =
        runTest(
            a(
                o(f("$wait", v(60000))),
                o(f("$probe", o(f("name", v("test")), f("topic", v("test")))))),
            list(message),
            (topic, msg) -> {
              names.add(msg.value.getString("name"));

              return completedFuture(true);
            });

    assertEquals(1, result.size());
    assertEquals(message, result.get(0).value);
    assertEquals(1, names.size());
    assertEquals("test", names.get(0));
  }
}
