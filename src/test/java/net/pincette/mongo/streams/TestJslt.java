package net.pincette.mongo.streams;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.util.Collections.list;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import javax.json.JsonObject;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestJslt extends Base {
  private void jslt(final String script) {
    final List<TestRecord<String, JsonObject>> result =
        runTest(a(o(f("$jslt", v(script)))), list(o(f(ID, v("0")), f("test", v(0)))));

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v("0")), f("test", v(1))), result.get(0).value());
  }

  @Test
  @DisplayName("$jslt 1")
  void jslt1() {
    jslt("resource:/test.jslt");
  }

  @Test
  @DisplayName("$jslt 2")
  void jslt2() {
    jslt("{\"test\": 1, *: ." + "}");
  }
}
