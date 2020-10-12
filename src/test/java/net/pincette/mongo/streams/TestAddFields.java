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

class TestAddFields extends Base {
  @Test
  @DisplayName("$addFields")
  @SuppressWarnings("java:S1192")
  void addFields() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(o(f("$addFields", o(f("test1", v(1)), f("test2", v(0)), f("test3.test", v(1)))))),
            list(o(f(ID, v("0")), f("test1", v(0)), f("test3", o(f("test", v(0)))))));

    assertEquals(1, result.size());
    assertEquals(
        o(f(ID, v("0")), f("test1", v(1)), f("test2", v(0)), f("test3", o(f("test", v(1))))),
        result.get(0).value());
  }
}
