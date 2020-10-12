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

class TestBucket extends Base {
  private void bucket(final String collection) {
    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$bucket",
                        o(
                            f("groupBy", v("$test")),
                            f("boundaries", a(v(0), v(10), v(20), v(30))),
                            f("default", v("other")),
                            f("output", o(f("count", o(f("$sum", v(1)))))),
                            f("_collection", v(collection)))))),
            list(
                o(f("test", v(0))),
                o(f("test", v(1))),
                o(f("test", v(5))),
                o(f("test", v(10))),
                o(f("test", v(12))),
                o(f("test", v(18))),
                o(f("test", v(19))),
                o(f("test", v(22))),
                o(f("test", v(26))),
                o(f("test", v(56))),
                o(f("test", v(60))),
                o(f("test", v(70)))));

    assertEquals(12, result.size());
    assertEquals(o(f(ID, v(0)), f("count", v(1))), result.get(0).value());
    assertEquals(o(f(ID, v(0)), f("count", v(2))), result.get(1).value());
    assertEquals(o(f(ID, v(0)), f("count", v(3))), result.get(2).value());
    assertEquals(o(f(ID, v(10)), f("count", v(1))), result.get(3).value());
    assertEquals(o(f(ID, v(10)), f("count", v(2))), result.get(4).value());
    assertEquals(o(f(ID, v(10)), f("count", v(3))), result.get(5).value());
    assertEquals(o(f(ID, v(10)), f("count", v(4))), result.get(6).value());
    assertEquals(o(f(ID, v(20)), f("count", v(1))), result.get(7).value());
    assertEquals(o(f(ID, v(20)), f("count", v(2))), result.get(8).value());
    assertEquals(o(f(ID, v("other")), f("count", v(1))), result.get(9).value());
    assertEquals(o(f(ID, v("other")), f("count", v(2))), result.get(10).value());
    assertEquals(o(f(ID, v("other")), f("count", v(3))), result.get(11).value());
  }

  @Test
  @DisplayName("$bucket 1")
  void bucket1() {
    bucket(null);
  }

  @Test
  @DisplayName("$bucket 2")
  void bucket2() {
    bucket("pincette-mongo-streams-test");
  }
}
