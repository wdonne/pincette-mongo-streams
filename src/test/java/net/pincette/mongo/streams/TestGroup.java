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

class TestGroup extends Base {
  @Test
  @DisplayName("$group $addToSet")
  void addToSet() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v(null)), f("items", o(f("$addToSet", v("$test")))))))),
            list(o(f("test", v(2))), o(f("test", v(1))), o(f("test", v(2))), o(f("test", v(0)))));

    assertEquals(3, result.size());
    assertEquals(o(f(ID, v(null)), f("items", a(v(2)))), result.get(0).value);
    assertEquals(o(f(ID, v(null)), f("items", a(v(1), v(2)))), result.get(1).value);
    assertEquals(o(f(ID, v(null)), f("items", a(v(0), v(1), v(2)))), result.get(2).value);
  }

  @Test
  @DisplayName("$group $avg")
  void avg() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v(null)), f("avg", o(f("$avg", v("$test")))))))),
            list(o(f("test", v(0))), o(f("test", v(1))), o(f("test", v(2))), o(f("test", v(3)))));

    assertEquals(4, result.size());
    assertEquals(o(f(ID, v(null)), f("avg", v(0.0))), result.get(0).value);
    assertEquals(o(f(ID, v(null)), f("avg", v(0.5))), result.get(1).value);
    assertEquals(o(f(ID, v(null)), f("avg", v(1.0))), result.get(2).value);
    assertEquals(o(f(ID, v(null)), f("avg", v(1.5))), result.get(3).value);
  }

  private void group(final String collection) {
    final JsonObject expression =
        collection != null
            ? o(f(ID, v("$test")), f("_collection", v(collection)))
            : o(f(ID, v("$test")));
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", expression))),
            list(
                o(f("test", v("0"))),
                o(f("test", v("0"))),
                o(f("test", v("1"))),
                o(f("test", v("0")))));

    assertEquals(2, result.size());
    assertEquals(o(f(ID, v("0"))), result.get(0).value);
    assertEquals(o(f(ID, v("1"))), result.get(1).value);
  }

  @Test
  @DisplayName("$group 1")
  void group1() {
    group(null);
  }

  @Test
  @DisplayName("$group 2")
  void group2() {
    group("pincette-mongo-streams-test");
  }

  @Test
  @DisplayName("$group $max")
  void max() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v(null)), f("max", o(f("$max", v("$test")))))))),
            list(o(f("test", v(0))), o(f("test", v(1))), o(f("test", v(2))), o(f("test", v(0)))));

    assertEquals(3, result.size());
    assertEquals(o(f(ID, v(null)), f("max", v(0))), result.get(0).value);
    assertEquals(o(f(ID, v(null)), f("max", v(1))), result.get(1).value);
    assertEquals(o(f(ID, v(null)), f("max", v(2))), result.get(2).value);
  }

  @Test
  @DisplayName("$group $mergeObjects 1")
  void mergeObjects1() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v(null)), f("merged", o(f("$mergeObjects", v("$test")))))))),
            list(o(f("test", v(null)))));

    assertEquals(1, result.size());
    assertEquals(o(f(ID, v(null)), f("merged", o())), result.get(0).value);
  }

  @Test
  @DisplayName("$group $mergeObjects 2")
  void mergeObjects2() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v(null)), f("merged", o(f("$mergeObjects", v("$test")))))))),
            list(
                o(f("test", o(f("test1", v(0))))),
                o(f("test", o(f("test2", v(0))))),
                o(f("test", o(f("test1", v(1)))))));

    assertEquals(3, result.size());
    assertEquals(o(f(ID, v(null)), f("merged", o(f("test1", v(0))))), result.get(0).value);
    assertEquals(
        o(f(ID, v(null)), f("merged", o(f("test1", v(0)), f("test2", v(0))))), result.get(1).value);
    assertEquals(
        o(f(ID, v(null)), f("merged", o(f("test1", v(1)), f("test2", v(0))))), result.get(2).value);
  }

  @Test
  @DisplayName("$group $min")
  void min() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v(null)), f("min", o(f("$min", v("$test")))))))),
            list(o(f("test", v(2))), o(f("test", v(1))), o(f("test", v(2))), o(f("test", v(0)))));

    assertEquals(3, result.size());
    assertEquals(o(f(ID, v(null)), f("min", v(2))), result.get(0).value);
    assertEquals(o(f(ID, v(null)), f("min", v(1))), result.get(1).value);
    assertEquals(o(f(ID, v(null)), f("min", v(0))), result.get(2).value);
  }

  @Test
  @DisplayName("$group $push")
  void push() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v(null)), f("items", o(f("$push", v("$test")))))))),
            list(o(f("test", v(2))), o(f("test", v(1))), o(f("test", v(2))), o(f("test", v(0)))));

    assertEquals(4, result.size());
    assertEquals(o(f(ID, v(null)), f("items", a(v(2)))), result.get(0).value);
    assertEquals(o(f(ID, v(null)), f("items", a(v(2), v(1)))), result.get(1).value);
    assertEquals(o(f(ID, v(null)), f("items", a(v(2), v(1), v(2)))), result.get(2).value);
    assertEquals(o(f(ID, v(null)), f("items", a(v(2), v(1), v(2), v(0)))), result.get(3).value);
  }

  @Test
  @DisplayName("$group $stdDevPop")
  void stdDevPop() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v(null)), f("stdDevPop", o(f("$stdDevPop", v("$test")))))))),
            list(o(f("test", v(0))), o(f("test", v(1))), o(f("test", v(2))), o(f("test", v(3)))));

    assertEquals(4, result.size());
    assertEquals(o(f(ID, v(null)), f("stdDevPop", v(1))), result.get(0).value);
    assertEquals(o(f(ID, v(null)), f("stdDevPop", v(2))), result.get(1).value);
    assertEquals(o(f(ID, v(null)), f("stdDevPop", v(3))), result.get(2).value);
    assertEquals(o(f(ID, v(null)), f("stdDevPop", v(4))), result.get(3).value);
  }

  @Test
  @DisplayName("$group $sum 1")
  void sum1() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v("$test")), f("sum", o(f("$sum", v(1)))))))),
            list(
                o(f("test", v("0"))),
                o(f("test", v("0"))),
                o(f("test", v("1"))),
                o(f("test", v("0")))));

    assertEquals(4, result.size());
    assertEquals(o(f(ID, v("0")), f("sum", v(1))), result.get(0).value);
    assertEquals(o(f(ID, v("0")), f("sum", v(2))), result.get(1).value);
    assertEquals(o(f(ID, v("1")), f("sum", v(1))), result.get(2).value);
    assertEquals(o(f(ID, v("0")), f("sum", v(3))), result.get(3).value);
  }

  @Test
  @DisplayName("$group $sum 2")
  void sum2() {
    final List<Message<String, JsonObject>> result =
        runTest(
            a(o(f("$group", o(f(ID, v("$test")), f("sum", o(f("$sum", v(1.5)))))))),
            list(
                o(f("test", v("0"))),
                o(f("test", v("0"))),
                o(f("test", v("1"))),
                o(f("test", v("0")))));

    assertEquals(4, result.size());
    assertEquals(o(f(ID, v("0")), f("sum", v(1.5))), result.get(0).value);
    assertEquals(o(f(ID, v("0")), f("sum", v(3))), result.get(1).value);
    assertEquals(o(f(ID, v("1")), f("sum", v(1.5))), result.get(2).value);
    assertEquals(o(f(ID, v("0")), f("sum", v(4.5))), result.get(3).value);
  }
}
