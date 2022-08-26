package net.pincette.mongo.streams;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Expression.replaceVariables;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.mongo.JsonClient.aggregationPublisher;
import static net.pincette.mongo.streams.Pipeline.LOOKUP;
import static net.pincette.mongo.streams.Pipeline.MATCH;
import static net.pincette.mongo.streams.Util.RETRY;
import static net.pincette.mongo.streams.Util.exceptionLogger;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Flatten.flatMap;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.retryPublisher;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;

import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.mongo.Features;
import net.pincette.rs.streams.Message;

/**
 * The <code>$lookup</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Lookup {
  private static final String AS = "as";
  private static final String CONNECTION_STRING = "connectionString";
  private static final String DATABASE = "database";
  private static final String FOREIGN_FIELD = "foreignField";
  private static final String FROM = "from";
  private static final String IN = "$in";
  private static final String INNER = "inner";
  private static final String LET = "let";
  private static final String LOCAL_FIELD = "localField";
  private static final String PIPELINE = "pipeline";
  private static final String UNWIND = "unwind";

  private Lookup() {}

  private static Optional<MongoDatabase> getDatabase(final JsonObject expression) {
    return ofNullable(expression.getString(CONNECTION_STRING, null))
        .flatMap(c -> ofNullable(expression.getString(DATABASE, null)).map(db -> pair(c, db)))
        .map(pair -> create(pair.first).getDatabase(pair.second));
  }

  private static CompletionStage<JsonArrayBuilder> lookup(
      final String collection, final JsonArray query, final Context context) {
    return tryForever(
        () ->
            aggregate(context.database.getCollection(collection), query)
                .thenApply(
                    list ->
                        list.stream()
                            .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)),
        LOOKUP,
        () -> "Collection " + collection + ", lookup: " + string(query),
        context);
  }

  private static Publisher<JsonObject> lookupPublisher(
      final String collection, final JsonArray query, final Context context) {
    return retryPublisher(
        () -> aggregationPublisher(context.database.getCollection(collection), query),
        RETRY,
        e -> exceptionLogger(e, "$lookup", context));
  }

  private static JsonArray query(final JsonObject expression) {
    return ofNullable(expression.getString(FOREIGN_FIELD, null))
        .map(field -> a(o(f(MATCH, o(f(field, o(f(IN, v("$$" + LOCAL_FIELD)))))))))
        .orElseGet(() -> expression.getJsonArray(PIPELINE));
  }

  private static Function<JsonObject, JsonArray> queryFunction(
      final JsonObject expression, final Context context) {
    final JsonArray query = query(expression);
    final Map<String, Function<JsonObject, JsonValue>> variables =
        variables(expression, context.features);

    return json -> replaceVariables(query, setVariables(variables, json)).asJsonArray();
  }

  private static Map<String, JsonValue> setVariables(
      final Map<String, Function<JsonObject, JsonValue>> variables, final JsonObject json) {
    return variables.entrySet().stream()
        .collect(toMap(Entry::getKey, e -> e.getValue().apply(json)));
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();
    final String as = expr.getString(AS);
    final String from = expr.getString(FROM);
    final boolean inner = expr.getBoolean(INNER, false);
    final Context localContext = getDatabase(expr).map(context::withDatabase).orElse(context);
    final Function<JsonObject, JsonArray> queryFunction = queryFunction(expr, context);
    final boolean unwind = expr.getBoolean(UNWIND, false);

    return unwind
        ? flatMap(
            m ->
                unwindResult(
                    m, lookupPublisher(from, queryFunction.apply(m.value), localContext), as))
        : pipe(mapAsync(
                (Message<String, JsonObject> m) ->
                    lookup(from, queryFunction.apply(m.value), localContext)
                        .thenApply(builder -> pair(m, builder))))
            .then(
                map(
                    pair ->
                        pair.first.withValue(
                            createObjectBuilder(pair.first.value).add(as, pair.second).build())))
            .then(filter(m -> !inner || !m.value.getJsonArray(as).isEmpty()));
  }

  private static JsonValue toArray(final JsonValue value) {
    return isArray(value) ? value : a(value);
  }

  private static Publisher<Message<String, JsonObject>> unwindResult(
      final Message<String, JsonObject> message,
      final Publisher<JsonObject> results,
      final String as) {
    return with(results)
        .map(
            result -> message.withValue(createObjectBuilder(message.value).add(as, result).build()))
        .get();
  }

  private static Map<String, Function<JsonObject, JsonValue>> variables(
      final JsonObject expression, final Features features) {
    return ofNullable(expression.getString(LOCAL_FIELD, null))
        .map(
            field ->
                map(
                    pair(
                        "$$" + LOCAL_FIELD,
                        wrapArray(function(createValue("$" + field), features)))))
        .orElseGet(
            () ->
                expression.getJsonObject(LET).entrySet().stream()
                    .collect(toMap(e -> "$$" + e.getKey(), e -> function(e.getValue(), features))));
  }

  private static Function<JsonObject, JsonValue> wrapArray(
      final Function<JsonObject, JsonValue> function) {
    return json -> toArray(function.apply(json));
  }
}
