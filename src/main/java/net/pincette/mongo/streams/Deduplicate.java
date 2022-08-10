package net.pincette.mongo.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.BsonUtil.toDocument;
import static net.pincette.mongo.Collection.findOne;
import static net.pincette.mongo.Collection.insertOne;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Pipeline.DEDUPLICATE;
import static net.pincette.mongo.streams.Util.ID;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.duplicateFilter;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;

import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The <code>$deduplicate</code> operator.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
class Deduplicate {
  private static final String CACHE_WINDOW = "cacheWindow";
  private static final String COLLECTION = "collection";
  private static final String EXPRESSION = "expression";
  private static final String TIMESTAMP = "_timestamp";

  private Deduplicate() {}

  private static CompletionStage<Boolean> exists(
      final MongoCollection<Document> collection, final Bson filter, final Context context) {
    return tryForever(
        () -> findOne(collection, filter, BsonDocument.class, null).thenApply(Optional::isPresent),
        DEDUPLICATE,
        context);
  }

  private static CompletionStage<Boolean> save(
      final MongoCollection<Document> collection, final BsonValue value, final Context context) {
    return tryForever(
        () ->
            insertOne(
                    collection,
                    toDocument(
                        new BsonDocument()
                            .append(ID, value)
                            .append(TIMESTAMP, new BsonInt64(now().toEpochMilli()))))
                .thenApply(result -> must(result, InsertOneResult::wasAcknowledged))
                .thenApply(result -> true),
        DEDUPLICATE,
        context);
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();
    final MongoCollection<Document> collection =
        context.database.getCollection(expr.getString(COLLECTION));
    final Function<JsonObject, JsonValue> fn = function(expr.get(EXPRESSION), context.features);

    return pipe(duplicateFilter(
            (Message<String, JsonObject> m) -> fn.apply(m.value),
            ofMillis(expr.getInt(CACHE_WINDOW, 60000))))
        .then(map(m -> pair(m, fromJson(fn.apply(m.value)))))
        .then(
            mapAsync(
                pair ->
                    exists(collection, eq(ID, pair.second), context)
                        .thenApply(result -> pair(pair, result))))
        .then(filter(pair -> !pair.second))
        .then(map(pair -> pair.first))
        .then(
            mapAsync(
                pair -> save(collection, pair.second, context).thenApply(result -> pair.first)));
  }
}
