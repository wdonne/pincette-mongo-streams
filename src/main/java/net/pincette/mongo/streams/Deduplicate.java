package net.pincette.mongo.streams;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.Collection.bulkWrite;
import static net.pincette.mongo.Collection.findOne;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Pipeline.DEDUPLICATE;
import static net.pincette.mongo.streams.Util.ID;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.rs.Commit.commit;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.duplicateFilter;
import static net.pincette.rs.Util.onCancelProcessor;
import static net.pincette.rs.Util.onCompleteProcessor;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.mongo.BsonUtil;
import net.pincette.rs.streams.Message;
import net.pincette.util.Pair;
import net.pincette.util.State;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The <code>$deduplicate</code> operator.
 *
 * @author Werner Donné
 * @since 3.0
 */
class Deduplicate {
  private static final String CACHE_WINDOW = "cacheWindow";
  private static final String COLLECTION = "collection";
  private static final String EXPRESSION = "expression";
  private static final String TIMESTAMP = "_timestamp";

  private Deduplicate() {}

  private static CompletionStage<Boolean> exists(
      final MongoCollection<Document> collection,
      final Bson filter,
      final BooleanSupplier stop,
      final Context context) {
    return tryForever(
        () -> findOne(collection, filter, BsonDocument.class, null).thenApply(Optional::isPresent),
        DEDUPLICATE,
        stop,
        () ->
            "Collection "
                + collection
                + ", findOne with filter: "
                + string(fromBson(toBsonDocument(filter))),
        context);
  }

  private static CompletionStage<Boolean> save(
      final MongoCollection<Document> collection,
      final List<BsonValue> values,
      final BooleanSupplier stop,
      final Context context) {
    return tryForever(
        () ->
            bulkWrite(collection, values.stream().map(Deduplicate::updateObject).toList())
                .thenApply(result -> must(result, BulkWriteResult::wasAcknowledged))
                .thenApply(result -> true),
        DEDUPLICATE,
        stop,
        () ->
            "Collection "
                + collection
                + ", save: "
                + string(from(values.stream().map(BsonUtil::fromBson))),
        context);
  }

  private static <T, U> Stream<U> second(final List<Pair<T, U>> pairs) {
    return pairs.stream().map(pair -> pair.second);
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();

    must(expr.containsKey(COLLECTION));

    final Duration cacheWindow = ofMillis(expr.getInt(CACHE_WINDOW, 3000));
    final MongoCollection<Document> collection =
        context.database.getCollection(expr.getString(COLLECTION));
    final Function<JsonObject, JsonValue> fn = function(expr.get(EXPRESSION), context.features);
    final State<Boolean> stop = new State<>(false);

    // The duplicate filter is needed because when downstream is buffered, the request size will
    // be larger than 1. An upstream batch may contain duplicates.
    return pipe(duplicateFilter((Message<String, JsonObject> m) -> fn.apply(m.value), cacheWindow))
        .then(map(m -> pair(m, fromJson(fn.apply(m.value)))))
        .then(
            mapAsyncSequential(
                pair ->
                    exists(collection, eq(ID, pair.second), stop::get, context)
                        .thenApply(result -> pair(pair, result))))
        .then(filter(pair -> !pair.second))
        .then(map(pair -> pair.first))
        .then(commit(list -> save(collection, second(list).toList(), stop::get, context)))
        .then(map(pair -> pair.first))
        .then(onCancelProcessor(() -> stop.set(true)))
        .then(onCompleteProcessor(() -> stop.set(true)));
  }

  private static UpdateOneModel<Document> updateObject(final BsonValue value) {
    return new UpdateOneModel<>(
        eq(ID, value),
        combine(set(ID, value), set(TIMESTAMP, new BsonInt64(now().toEpochMilli()))),
        new UpdateOptions().upsert(true));
  }
}
