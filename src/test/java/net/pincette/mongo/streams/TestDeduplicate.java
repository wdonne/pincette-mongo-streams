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

class TestDeduplicate extends Base {
  private static final String COLLECTION = "pincette-mongo-streams-dedup-test";

  @Test
  @DisplayName("$deduplicate")
  void deduplicate() {
    run(60000);
    run(0);
  }

  private void run(final long window) {
    drop(COLLECTION);

    final JsonObject o1 = o(f(ID, v("0")));
    final JsonObject o2 = o(f(ID, o(f("test", v("0")))));
    final List<Message<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$deduplicate",
                        o(
                            f("expression", v("$" + ID)),
                            f("collection", v(COLLECTION)),
                            f("cacheWindow", v(window)))))),
            list(o1, o2, o1, o2));

    assertEquals(2, result.size());
    assertEquals(o1, result.get(0).value);
    assertEquals(o2, result.get(1).value);
  }
}
