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

class TestScript extends Base {
  private void jq(final String script) {
    script(script, "$jq");
  }

  private void jslt(final String script) {
    script(script, "$jslt");
  }

  private void script(final String script, final String stage) {
    final List<Message<String, JsonObject>> result =
        runTest(a(o(f(stage, v(script)))), list(o(f(ID, v("0")), f("test", v(0)))));

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v("0")), f("test", v(1))), result.get(0).value);
  }

  @Test
  @DisplayName("$jq 1")
  void jq1() {
    jq("resource:/test.jq");
  }

  @Test
  @DisplayName("$jq 2")
  void jq2() {
    jq(". + {test: 1}");
  }

  @Test
  @DisplayName("$jslt 1")
  void jslt1() {
    jslt("resource:/test.jslt");
  }

  @Test
  @DisplayName("$jslt 2")
  void jslt2() {
    jslt("{\"test\": 1, *: .}");
  }
}
