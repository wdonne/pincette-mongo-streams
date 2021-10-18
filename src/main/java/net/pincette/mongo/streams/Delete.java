package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.deleteMany;
import static net.pincette.mongo.streams.Util.matchFields;
import static net.pincette.mongo.streams.Util.matchQuery;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.util.Util.must;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.Set;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.apache.kafka.streams.kstream.KStream;
import org.bson.Document;

/**
 * The <code>$delete</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Delete {
  private static final String FROM = "from";

  private Delete() {}

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();
    final MongoCollection<Document> collection =
        context.database.getCollection(expr.getString(FROM));
    final Set<String> fields = matchFields(expr, null);

    assert !fields.isEmpty();

    return stream
        .mapValues(
            v ->
                matchQuery(v, fields)
                    .map(
                        query ->
                            tryForever(
                                () ->
                                    deleteMany(collection, fromJson(query))
                                        .thenApply(
                                            result -> must(result, DeleteResult::wasAcknowledged)),
                                "$delete",
                                context))
                    .map(result -> v)
                    .orElse(null))
        .filter((k, v) -> v != null);
  }
}
