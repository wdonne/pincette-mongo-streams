package net.pincette.mongo.streams;

import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.deleteMany;
import static net.pincette.mongo.streams.Pipeline.DELETE;
import static net.pincette.mongo.streams.Util.matchFields;
import static net.pincette.mongo.streams.Util.matchQuery;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.onCancelProcessor;
import static net.pincette.rs.Util.onCompleteProcessor;
import static net.pincette.util.Util.must;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.Set;
import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;
import net.pincette.util.State;
import org.bson.Document;

/**
 * The <code>$delete</code> operator.
 *
 * @author Werner Donné
 */
class Delete {
  private static final String FROM = "from";

  private Delete() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();
    final MongoCollection<Document> collection =
        context.database.getCollection(expr.getString(FROM));
    final Set<String> fields = matchFields(expr, null);
    final State<Boolean> stop = new State<>(false);

    assert !fields.isEmpty();

    return pipe(map(
            (Message<String, JsonObject> m) ->
                m.withValue(
                    matchQuery(m.value, fields)
                        .map(
                            query ->
                                tryForever(
                                    () ->
                                        deleteMany(collection, fromJson(query))
                                            .thenApply(
                                                result ->
                                                    must(result, DeleteResult::wasAcknowledged)),
                                    DELETE,
                                    stop::get,
                                    () -> "Collection " + collection + ", delete: " + string(query),
                                    context))
                        .map(result -> m.value)
                        .orElse(null))))
        .then(filter(m -> m.value != null))
        .then(onCancelProcessor(() -> stop.set(true)))
        .then(onCompleteProcessor(() -> stop.set(true)));
  }
}
