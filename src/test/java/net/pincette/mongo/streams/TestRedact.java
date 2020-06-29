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

public class TestRedact extends Base {
  @Test
  @DisplayName("$redact")
  public void redact() {
    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$redact",
                        o(
                            f(
                                "$switch",
                                o(
                                    f(
                                        "branches",
                                        a(
                                            o(
                                                f("case", o(f("$eq", a(v("$test"), v(1))))),
                                                f("then", v("$$KEEP"))),
                                            o(
                                                f("case", o(f("$eq", a(v("$test"), v(2))))),
                                                f("then", v("$$PRUNE"))))),
                                    f("default", v("$$DESCEND")))))))),
            list(
                o(
                    f(ID, v("0")),
                    f("test", v(0)),
                    f("test1", o(f("test", v(0)), f("v", v(0)))),
                    f("test2", o(f("test", v(1)), f("v", v(0)), f("sub", o(f("test", v(2)))))),
                    f("test3", o(f("test", v(2)), f("v", v(0)))),
                    f(
                        "test4",
                        a(
                            o(f("test", v(0)), f("v", v(0))),
                            o(f("test", v(1)), f("v", v(0)), f("sub", o(f("test", v(2))))),
                            o(f("test", v(2)), f("v", v(0))))))));

    assertEquals(1, result.size());
    assertEquals(
        o(
            f(ID, v("0")),
            f("test", v(0)),
            f("test1", o(f("test", v(0)), f("v", v(0)))),
            f("test2", o(f("test", v(1)), f("v", v(0)), f("sub", o(f("test", v(2)))))),
            f(
                "test4",
                a(
                    o(f("test", v(0)), f("v", v(0))),
                    o(f("test", v(1)), f("v", v(0)), f("sub", o(f("test", v(2)))))))),
        result.get(0).value());
  }
}
