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

class TestAddFields extends Base {
  @Test
  @DisplayName("$addFields 1")
  void addFields1() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$addFields",
                        o(
                            f("test1", v(1)),
                            f("test2", v(0)),
                            f("test3.test", v(1)),
                            f("test4.test.test1", v(2)),
                            f("test4.test.test2", v(3)))))),
            list(
                o(
                    f(ID, v("0")),
                    f("test1", v(0)),
                    f("test3", o(f("test", v(0)))),
                    f("test.test", v(0)))));

    assertEquals(1, result.size());
    assertEquals(
        o(
            f(ID, v("0")),
            f("test1", v(1)),
            f("test2", v(0)),
            f("test3", o(f("test", v(1)))),
            f("test4", o(f("test", o(f("test1", v(2)), f("test2", v(3)))))),
            f("test.test", v(0))),
        result.getFirst().value);
  }

  @Test
  @DisplayName("$addFields 2")
  void addFields2() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$addFields", o(f("test4.test.test2", v(3)))))),
            list(o(f(ID, v("0")), f("test1", v(0)), f("test3", o(f("test", v(0)))))));

    assertEquals(1, result.size());
    assertEquals(
        o(
            f(ID, v("0")),
            f("test1", v(0)),
            f("test3", o(f("test", v(0)))),
            f("test4", o(f("test", o(f("test2", v(3))))))),
        result.getFirst().value);
  }

  @Test
  @DisplayName("$addFields 3")
  void addFields3() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$addFields", o(f("test3.test1.test2", v(3)))))),
            list(o(f(ID, v("0")), f("test1", v(0)), f("test3", o(f("test", v(0)))))));

    assertEquals(1, result.size());
    assertEquals(
        o(
            f(ID, v("0")),
            f("test1", v(0)),
            f("test3", o(f("test", v(0)), f("test1", o(f("test2", v(3))))))),
        result.getFirst().value);
  }
}
