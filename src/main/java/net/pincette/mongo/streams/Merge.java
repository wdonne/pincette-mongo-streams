package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.insert;
import static net.pincette.mongo.streams.Util.RETRY;
import static net.pincette.mongo.streams.Util.exceptionLogger;
import static net.pincette.mongo.streams.Util.matchFields;
import static net.pincette.mongo.streams.Util.matchQuery;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.rethrow;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.mongo.JsonClient;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.bson.Document;

/**
 * The <code>$merge</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Merge {
  private static final String FAIL = "fail";
  private static final String ID = "_id";
  private static final String INSERT = "insert";
  private static final String INTO = "into";
  private static final String KEEP_EXISTING = "keepExisting";
  private static final String KEY = "key";
  private static final String MERGE_FIELD = "merge";
  private static final String REPLACE = "replace";
  private static final String WHEN_MATCHED = "whenMatched";
  private static final String WHEN_NOT_MATCHED = "whenNotMatched";

  private Merge() {}

  private static JsonObject addId(final JsonObject json) {
    return json.containsKey(ID)
        ? json
        : createObjectBuilder(json).add(ID, randomUUID().toString()).build();
  }

  private static JsonObject addId(final JsonObject json, final JsonValue value) {
    return createObjectBuilder(json).add(ID, value).build();
  }

  private static FailException exception(final JsonObject expression) {
    return new FailException("$merge " + string(expression) + " failed");
  }

  private static String getWhenMatched(final JsonObject expression) {
    return expression.getString(WHEN_MATCHED, MERGE_FIELD);
  }

  private static JsonObject process(
      final JsonObject fromStream,
      final JsonValue key,
      final JsonObject query,
      final JsonObject expression,
      final MongoCollection<Document> collection,
      final Context context) {
    return findOne(collection, query)
        .thenComposeAsync(
            found ->
                found
                    .map(f -> processExisting(fromStream, f, expression, collection))
                    .orElseGet(() -> processNew(addId(fromStream, key), expression, collection)))
        .exceptionally(
            t -> {
              exceptionLogger(t.getCause(), "$merge", context);

              if (t.getCause() instanceof FailException) {
                rethrow(t.getCause());
              }

              return composeAsyncAfter(
                      () ->
                          completedFuture(
                              process(fromStream, key, query, expression, collection, context)),
                      RETRY)
                  .toCompletableFuture()
                  .join();
            })
        .toCompletableFuture()
        .join();
  }

  private static CompletionStage<JsonObject> processExisting(
      final JsonObject fromStream,
      final JsonObject fromCollection,
      final JsonObject expression,
      final MongoCollection<Document> collection) {
    switch (getWhenMatched(expression)) {
      case FAIL:
        throw exception(expression);
      case KEEP_EXISTING:
        return completedFuture(fromCollection);
      case MERGE_FIELD:
        return update(
            collection,
            copy(fromStream, createObjectBuilder(fromCollection)).build(),
            fromCollection);
      case REPLACE:
        return update(collection, fromStream, fromCollection);
      default:
        return completedFuture(emptyObject());
    }
  }

  private static CompletionStage<JsonObject> processNew(
      final JsonObject fromStream,
      final JsonObject expression,
      final MongoCollection<Document> collection) {
    switch (expression.getString(WHEN_NOT_MATCHED, INSERT)) {
      case FAIL:
        throw exception(expression);
      case INSERT:
        return Optional.of(addId(fromStream))
            .map(json -> update(collection, json, null))
            .orElseGet(() -> completedFuture(null));
      default:
        return completedFuture(emptyObject());
    }
  }

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();
    final MongoCollection<Document> collection =
        context.database.getCollection(expr.getString(INTO));
    final Set<String> fields = matchFields(expr, ID);
    final Function<JsonObject, JsonValue> key =
        function(
            getValue(expr, "/" + KEY).orElseGet(() -> createValue("$" + ID)), context.features);

    return stream
        .mapValues(
            v ->
                matchQuery(v, fields)
                    .map(query -> process(v, key.apply(v), query, expr, collection, context))
                    .map(
                        newValue ->
                            ofNullable(v.get(ID))
                                .filter(id -> !newValue.isEmpty())
                                .map(id -> addId(newValue, id))
                                .orElse(newValue))
                    .orElseThrow(() -> exception(expr)))
        .filter((k, v) -> !v.isEmpty())
        .map((k, v) -> new KeyValue<>(ofNullable(v.get(ID)).map(Util::generateKey).orElse(k), v));
  }

  private static CompletionStage<JsonObject> update(
      final MongoCollection<Document> collection,
      final JsonObject json,
      final JsonObject existing) {
    return (existing != null
            ? JsonClient.update(collection, existing, addId(json, existing.get(ID)))
            : insert(collection, json))
        .thenApply(result -> must(result, r -> r))
        .thenApply(result -> json);
  }

  private static class FailException extends RuntimeException {
    private FailException(final String message) {
      super(message);
    }
  }
}
