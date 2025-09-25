package net.pincette.mongo.streams;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.util.Collections.list;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import javax.json.JsonObject;
import net.pincette.rs.streams.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestSetKey extends Base {
  private static final JsonObject MESSAGE = o(f(ID, v("0")), f("test", v("1")));
  private static final List<JsonObject> MESSAGES = list(MESSAGE);

  @Test
  @DisplayName("$setKey")
  void count() {
    final List<Message<String, JsonObject>> result =
        runTest(a(o(f("$setKey", v("$test")))), MESSAGES);

    assertEquals(1, result.size());
    assertEquals(MESSAGE, result.getFirst().value);
    assertEquals("1", result.getFirst().key);
  }
}
