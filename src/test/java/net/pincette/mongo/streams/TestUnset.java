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

class TestUnset extends Base {
  private static final List<JsonObject> MESSAGES =
      list(o(f(ID, v("0")), f("test1", v(0)), f("test2", o(f("test", v(0))))));

  @Test
  @DisplayName("$unset 1")
  void unset1() {
    final List<Message<String, JsonObject>> result =
        runTest(a(o(f("$unset", v("test1")))), MESSAGES);

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v("0")), f("test2", o(f("test", v(0))))), result.get(0).value);
  }

  @Test
  @DisplayName("$unset 2")
  void unset2() {
    final List<Message<String, JsonObject>> result =
        runTest(a(o(f("$unset", a(v("test1"), v("test2.test"))))), MESSAGES);

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v("0")), f("test2", o())), result.get(0).value);
  }
}
