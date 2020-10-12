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

class TestProject extends Base {
  private static final List<JsonObject> MESSAGES =
      list(
          o(
              f(ID, v("0")),
              f("test1", v(0)),
              f("test2", v(0)),
              f("test3", o(f("test1", v(0)), f("test2", v(0)))),
              f("test4", o(f("test1", v(0)), f("test2", v(0)), f("test3", v(0)))),
              f("test5", o(f("test", v(0))))));

  @Test
  @DisplayName("$project exclude 1")
  void exclude1() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(o(f("$project", o(f("test3", v(0)), f("test4", v(false)), f("test5", v(0)))))),
            MESSAGES);

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v("0")), f("test1", v(0)), f("test2", v(0))), result.get(0).value());
  }

  @Test
  @DisplayName("$project exclude 2")
  void exclude2() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$project",
                        o(
                            f("test3.test2", v(0)),
                            f("test4", o(f("test3", v(false)))),
                            f("test5", v(0)))))),
            MESSAGES);

    assertEquals(1, result.size());
    assertEquals(
        o(
            f(ID, v("0")),
            f("test1", v(0)),
            f("test2", v(0)),
            f("test3", o(f("test1", v(0)))),
            f("test4", o(f("test1", v(0)), f("test2", v(0))))),
        result.get(0).value());
  }

  @Test
  @DisplayName("$project exclude 3")
  void exclude3() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$project",
                        o(
                            f(
                                "test",
                                o(
                                    f(
                                        "$cond",
                                        o(
                                            f("if", o(f("$eq", a(v(0), v("$test"))))),
                                            f("then", v("$$REMOVE")),
                                            f("else", v("$test")))))))))),
            list(o(f(ID, v("0")), f("test", v(0))), o(f(ID, v("1")), f("test", v(1)))));

    assertEquals(2, result.size());
    assertEquals(o(f(ID, v("0"))), result.get(0).value());
    assertEquals(o(f(ID, v("1")), f("test", v(1))), result.get(1).value());
  }

  @Test
  @DisplayName("$project include 1")
  void include1() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(a(o(f("$project", o(f("test1", v(1)), f("test2", v(true)))))), MESSAGES);

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v("0")), f("test1", v(0)), f("test2", v(0))), result.get(0).value());
  }

  @Test
  @DisplayName("$project include 2")
  void include2() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(a(o(f("$project", o(f("test1", v(1)), f(ID, v(false)))))), MESSAGES);

    assertEquals(1, result.size());
    assertEquals(o(f("test1", v(0))), result.get(0).value());
  }

  @Test
  @DisplayName("$project include 3")
  void include3() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$project",
                        o(
                            f("test1", v(10)),
                            f(ID, v(false)),
                            f("test3.test1", v(1)),
                            f("test3", o(f("test2", v(1)))),
                            f("test4", o(f("test2", v(10)))),
                            f("test4.test3", v(10)),
                            f("test5", v(1)))))),
            MESSAGES);

    assertEquals(1, result.size());
    assertEquals(
        o(
            f("test1", v(10)),
            f("test3", o(f("test1", v(0)), f("test2", v(0)))),
            f("test4", o(f("test1", v(0)), f("test2", v(10)), f("test3", v(10)))),
            f("test5", o(f("test", v(0))))),
        result.get(0).value());
  }
}
