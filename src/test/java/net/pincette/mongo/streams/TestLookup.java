package net.pincette.mongo.streams;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Util.must;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.List;
import java.util.Optional;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestLookup extends Base {
  private static final String COLLECTION = "pincette-mongo-streams-test";
  private static final String OTHER = "other";
  private static final String TEST = "test";
  private static final JsonObject MESSAGE1 = o(f(ID, v("0")), f(TEST, v(0)));
  private static final JsonObject MESSAGE2 = o(f(ID, v("1")), f(TEST, v(1)));

  private static JsonValue sort(final JsonValue value) {
    return isArray(value)
        ? value.asJsonArray().stream()
            .sorted(net.pincette.mongo.Util::compare)
            .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
            .build()
        : value;
  }

  private JsonObject lookup(final JsonObject message) {
    return lookup(message, false, 1);
  }

  private JsonObject lookup(final JsonObject message, final boolean inner, final int resultSize) {
    prepare();

    final List<Message<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$lookup",
                        o(
                            f("from", v(COLLECTION)),
                            f("inner", v(inner)),
                            f("localField", v(TEST)),
                            f("foreignField", v(TEST)),
                            f("as", v(OTHER)))))),
            list(message));

    assertEquals(resultSize, result.size());

    return Optional.of(result)
        .filter(r -> !r.isEmpty())
        .map(r -> r.get(0).value)
        .map(
            json ->
                copy(json, createObjectBuilder())
                    .add(OTHER, sort(json.getJsonArray(OTHER)))
                    .build())
        .orElse(null);
  }

  private JsonObject lookupPipeline(final JsonObject message) {
    prepare();

    final List<Message<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$lookup",
                        o(
                            f("from", v(COLLECTION)),
                            f("let", o(f("var", v("$test")))),
                            f("pipeline", a(o(f("$match", o(f(TEST, v("$$var"))))))),
                            f("as", v(OTHER)))))),
            list(message));

    assertEquals(1, result.size());

    final JsonObject json = result.get(0).value;

    return copy(json, createObjectBuilder()).add(OTHER, sort(json.getJsonArray(OTHER))).build();
  }

  @Test
  @DisplayName("$lookup 1")
  void lookup1() {
    assertEquals(
        o(f(ID, v("0")), f(TEST, v(0)), f(OTHER, a(MESSAGE1))),
        lookup(o(f(ID, v("0")), f(TEST, v(0)))));
  }

  @Test
  @DisplayName("$lookup 2")
  void lookup2() {
    assertEquals(
        o(f(ID, v("0")), f(TEST, v(1)), f(OTHER, a(MESSAGE2))),
        lookup(o(f(ID, v("0")), f(TEST, v(1)))));
  }

  @Test
  @DisplayName("$lookup 3")
  void lookup3() {
    assertEquals(
        o(f(ID, v("0")), f(TEST, a(v(0), v(1))), f(OTHER, sort(a(MESSAGE1, MESSAGE2)))),
        lookup(o(f(ID, v("0")), f(TEST, a(v(0), v(1))))));
  }

  @Test
  @DisplayName("$lookup 4")
  void lookup4() {
    assertEquals(
        o(f(ID, v("0")), f(TEST, v(2)), f(OTHER, a())), lookup(o(f(ID, v("0")), f(TEST, v(2)))));
  }

  @Test
  @DisplayName("$lookup 5")
  void lookup5() {
    assertNull(lookup(o(f(ID, v("0")), f(TEST, v(2))), true, 0));
  }

  @Test
  @DisplayName("$lookup 6")
  void lookup6() {
    assertEquals(
        o(f(ID, v("0")), f(TEST, v(0)), f(OTHER, a(MESSAGE1))),
        lookup(o(f(ID, v("0")), f(TEST, v(0))), true, 1));
  }

  @Test
  @DisplayName("$lookup 7")
  void lookup7() {
    assertEquals(
        o(f(ID, v("0")), f(TEST, v(0)), f(OTHER, a(MESSAGE1))),
        lookupPipeline(o(f(ID, v("0")), f(TEST, v(0)))));
  }

  @Test
  @DisplayName("$lookup 8")
  void lookup8() {
    assertEquals(
        o(f(ID, v("0")), f(TEST, v(1)), f(OTHER, a(MESSAGE2))),
        lookupPipeline(o(f(ID, v("0")), f(TEST, v(1)))));
  }

  @Test
  @DisplayName("$lookup 9")
  void lookup9() {
    assertEquals(
        o(f(ID, v("0")), f(TEST, v(2)), f(OTHER, a())),
        lookupPipeline(o(f(ID, v("0")), f(TEST, v(2)))));
  }

  @Test
  @DisplayName("$lookup 10")
  void lookup10() {
    prepare();

    final List<Message<String, JsonObject>> result =
        runTest(
            a(
                o(
                    f(
                        "$lookup",
                        o(
                            f("from", v(COLLECTION)),
                            f("let", o(f("var", v("$test")))),
                            f("pipeline", a(o(f("$match", o(f(TEST, o(f("$gte", v("$$var"))))))))),
                            f("unwind", v(true)),
                            f("as", v(OTHER))))),
                o(f("$project", o(f(OTHER, v(1)))))),
            list(o(f(TEST, v(0)))));

    assertEquals(2, result.size());

    assertEquals(
        list(MESSAGE1, MESSAGE2),
        result.stream()
            .map(m -> m.value)
            .map(v -> v.getJsonObject(OTHER))
            .sorted(comparing(v -> v.getString(ID)))
            .collect(toList()));
  }

  private void prepare() {
    drop(COLLECTION);

    final MongoCollection<Document> collection = resources.database.getCollection(COLLECTION);

    update(collection, MESSAGE1)
        .thenApply(result -> must(result, r -> r))
        .thenComposeAsync(result -> update(collection, MESSAGE2))
        .thenApply(result -> must(result, r -> r))
        .toCompletableFuture()
        .join();
  }
}
