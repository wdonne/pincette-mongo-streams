package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.insert;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.mongo.streams.Util.matchFields;
import static net.pincette.mongo.streams.Util.matchQuery;
import static net.pincette.util.Util.must;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.util.Util.GeneralException;
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

  private static GeneralException exception(final JsonObject expression) {
    return new GeneralException("$merge " + string(expression) + " failed");
  }

  private static CompletionStage<JsonObject> processExisting(
      final JsonObject fromStream,
      final JsonObject fromCollection,
      final JsonObject expression,
      final MongoCollection<Document> collection) {
    switch (expression.getString(WHEN_MATCHED, MERGE_FIELD)) {
      case FAIL:
        throw (exception(expression));
      case KEEP_EXISTING:
        return completedFuture(fromCollection);
      case MERGE_FIELD:
        final JsonObject merged =
            copy(fromStream, createObjectBuilder(fromCollection))
                .add(ID, fromCollection.getString(ID))
                .build();
        return update(collection, merged, merged.getString(ID))
            .thenApply(result -> must(result, r -> r))
            .thenApply(result -> merged);
      case REPLACE:
        return update(collection, fromStream, fromCollection.getString(ID))
            .thenApply(result -> must(result, r -> r))
            .thenApply(
                result ->
                    createObjectBuilder(fromStream).add(ID, fromCollection.getString(ID)).build());
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
        throw (exception(expression));
      case INSERT:
        return insert(collection, addId(fromStream))
            .thenApply(result -> must(result, r -> r))
            .thenApply(result -> fromStream);
      default:
        return completedFuture(emptyObject());
    }
  }

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    assert isObject(expression);

    final JsonObject expr = expression.asJsonObject();
    final MongoCollection<Document> collection =
        context.database.getCollection(expr.getString(INTO));
    final Set<String> fields = matchFields(expr, ID);

    return stream
        .mapValues(
            v ->
                matchQuery(v, fields)
                    .map(
                        query ->
                            findOne(collection, query)
                                .thenComposeAsync(
                                    found ->
                                        found
                                            .map(f -> processExisting(v, f, expr, collection))
                                            .orElseGet(() -> processNew(v, expr, collection)))
                                .toCompletableFuture()
                                .join())
                    .orElseThrow(() -> exception(expr)))
        .filter((k, v) -> !v.isEmpty())
        .map((k, v) -> new KeyValue<>(ofNullable(v.get(ID)).map(Util::generateKey).orElse(k), v));
  }
}
