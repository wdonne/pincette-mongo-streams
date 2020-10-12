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

class TestMatch extends Base {
  @Test
  @DisplayName("$match")
  void match() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(o(f("$match", o(f("test", v(0)))))),
            list(o(f(ID, v("0")), f("test", v(0))), o(f(ID, v("1")), f("test", v(1)))));

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v("0")), f("test", v(0))), result.get(0).value());
  }
}
