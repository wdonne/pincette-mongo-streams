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

class TestCount extends Base {
  @Test
  @DisplayName("$count")
  void count() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(a(o(f("$count", v("test")))), list(o(f(ID, v("0"))), o(f(ID, v("1")))));

    assertEquals(2, result.size());
    assertEquals(o(f("test", v(1))), result.get(0).value());
    assertEquals(o(f("test", v(2))), result.get(1).value());
  }
}
