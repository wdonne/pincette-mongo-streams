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

class TestPer extends Base {
  @Test
  @DisplayName("$per")
  void per() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$per", o(f("amount", v(2)), f("as", v("test")))))),
            list(o(f("test", v(0))), o(f("test", v(1))), o(f("test", v(2)))));

    assertEquals(2, result.size());
    assertEquals(o(f("test", a(o(f("test", v(0))), o(f("test", v(1)))))), result.get(0).value);
    assertEquals(o(f("test", a(o(f("test", v(2)))))), result.get(1).value);
  }
}
