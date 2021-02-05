package net.pincette.mongo.streams;

import static com.mongodb.client.model.Filters.eq;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Util.must;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import net.pincette.util.Util.GeneralException;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestMerge extends Base {
  private static final String COLLECTION = "pincette-mongo-streams-test";
  private static final JsonObject MESSAGE1 = o(f(ID, v("0")));
  private static final JsonObject MESSAGE2 =
      o(f(ID, v("0")), f("f1", v("0")), f("f2", v("1")), f("test", v(0)));
  private static final JsonObject NEW_MESSAGE =
      o(f(ID, v("0")), f("f1", v("0")), f("f2", v("1")), f("test", v(1)));

  @Test
  @DisplayName("$merge 1")
  void merge1() {
    drop(COLLECTION);

    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$merge",
                        o(
                            f("into", v(COLLECTION)),
                            f("on", v(ID)),
                            f("whenNotMatched", v("insert")))))),
            list(MESSAGE1));

    assertEquals(1, result.size());
    assertEquals(MESSAGE1, result.get(0).value());
    assertEquals(
        MESSAGE1,
        findOne(resources.database.getCollection(COLLECTION), eq(ID, "0"))
            .toCompletableFuture()
            .join()
            .orElseGet(JsonUtil::emptyObject));
  }

  @Test
  @DisplayName("$merge 2")
  void merge2() {
    drop(COLLECTION);

    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$merge",
                        o(
                            f("into", v(COLLECTION)),
                            f("on", v(ID)),
                            f("whenNotMatched", v("discard")))))),
            list(MESSAGE1));

    assertEquals(0, result.size());
    assertFalse(
        findOne(resources.database.getCollection(COLLECTION), eq(ID, "0"))
            .toCompletableFuture()
            .join()
            .isPresent());
  }

  @Test
  @DisplayName("$merge 3")
  void merge3() {
    drop(COLLECTION);

    assertThrows(
        GeneralException.class,
        () ->
            runTest(
                a(
                    o(
                        f(
                            "$merge",
                            o(
                                f("into", v(COLLECTION)),
                                f("on", v(ID)),
                                f("whenNotMatched", v("fail")))))),
                list(MESSAGE1)));
  }

  @Test
  @DisplayName("$merge 4")
  void merge4() {
    mergeExisting("replace", NEW_MESSAGE);
  }

  @Test
  @DisplayName("$merge 5")
  void merge5() {
    mergeExisting("keepExisting", MESSAGE2);
  }

  @Test
  @DisplayName("$merge 6")
  void merge6() {
    mergeExisting("merge", NEW_MESSAGE);
  }

  @Test
  @DisplayName("$merge 7")
  void merge7() {
    drop(COLLECTION);

    update(resources.database.getCollection(COLLECTION), MESSAGE2)
        .thenApply(result -> must(result, r -> r))
        .toCompletableFuture()
        .join();

    assertThrows(
        GeneralException.class,
        () ->
            runTest(
                a(
                    o(
                        f(
                            "$merge",
                            o(
                                f("into", v(COLLECTION)),
                                f("on", v(ID)),
                                f("whenMatched", v("fail")))))),
                list(NEW_MESSAGE)));
  }

  private void mergeExisting(final String action, final JsonObject expected) {
    drop(COLLECTION);

    update(
            resources.database.getCollection(COLLECTION),
            createObjectBuilder(MESSAGE2).add(ID, o(f("f1", v("0")), f("f2", v("1")))).build())
        .thenApply(result -> must(result, r -> r))
        .toCompletableFuture()
        .join();

    final List<TestRecord<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$merge",
                        o(
                            f("into", v(COLLECTION)),
                            f("on", a(v("f1"), v("f2"))),
                            f("key", o(f("f1", v("$f1")), f("f2", v("$f2")))),
                            f("whenMatched", v(action)))))),
            list(NEW_MESSAGE));

    assertEquals(1, result.size());
    assertEquals(expected, result.get(0).value());
    assertEquals(
        expected,
        findOne(
                resources.database.getCollection(COLLECTION),
                eq(ID, fromJson(o(f("f1", v("0")), f("f2", v("1"))))))
            .thenApply(
                res -> res.map(r -> createObjectBuilder(r).add(ID, NEW_MESSAGE.get(ID)).build()))
            .toCompletableFuture()
            .join()
            .orElseGet(JsonUtil::emptyObject));
  }
}
