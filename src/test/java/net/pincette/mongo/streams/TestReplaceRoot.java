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

class TestReplaceRoot extends Base {
  @Test
  @DisplayName("$replaceRoot")
  void replaceRoot() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$replaceRoot", o(f("newRoot", v("$test.test")))))),
            list(o(f(ID, v("0")), f("test", o(f("test", o(f(ID, v("1")), f("test", v(0)))))))));

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v("1")), f("test", v(0))), result.getFirst().value);
    assertEquals("1", result.getFirst().key);
  }
}
