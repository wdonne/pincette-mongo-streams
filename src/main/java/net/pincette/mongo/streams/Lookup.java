package net.pincette.mongo.streams;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static net.pincette.json.Factory.a;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Expression.replaceVariables;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.mongo.JsonClient.aggregationPublisher;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.iterate;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;

import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import net.pincette.mongo.Features;
import org.apache.kafka.streams.kstream.KStream;
import org.reactivestreams.Publisher;

/**
 * The <code>$lookup</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Lookup {
  private static final String AS = "as";
  private static final String CONNECTION_STRING = "connectionString";
  private static final String DATABASE = "database";
  private static final String EXISTS = "$exists";
  private static final String FOREIGN_FIELD = "foreignField";
  private static final String FROM = "from";
  private static final String IN = "$in";
  private static final String INNER = "inner";
  private static final String LET = "let";
  private static final String MATCH = "$match";
  private static final String LOCAL_FIELD = "localField";
  private static final String OR = "$or";
  private static final String PIPELINE = "pipeline";
  private static final String UNWIND = "unwind";

  private Lookup() {}

  private static Optional<MongoDatabase> getDatabase(final JsonObject expression) {
    return ofNullable(expression.getString(CONNECTION_STRING, null))
        .flatMap(c -> ofNullable(expression.getString(DATABASE, null)).map(db -> pair(c, db)))
        .map(pair -> create(pair.first).getDatabase(pair.second));
  }

  private static JsonArrayBuilder lookup(
      final String collection, final JsonArray query, final Context context) {
    return aggregate(context.database.getCollection(collection), query)
        .thenApply(
            list ->
                list.stream().reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1))
        .toCompletableFuture()
        .join();
  }

  private static Publisher<JsonObject> lookupPublisher(
      final String collection, final JsonArray query, final Context context) {
    return aggregationPublisher(context.database.getCollection(collection), query);
  }

  private static JsonArray query(final JsonObject expression) {
    return ofNullable(expression.getString(FOREIGN_FIELD, null))
        .map(
            field ->
                createArrayBuilder()
                    .add(
                        createObjectBuilder()
                            .add(
                                MATCH,
                                wrapOuter(
                                    expression,
                                    field,
                                    createObjectBuilder()
                                        .add(
                                            field,
                                            createObjectBuilder().add(IN, "$$" + LOCAL_FIELD)))))
                    .build())
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

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    assert isObject(expression);

    final JsonObject expr = expression.asJsonObject();
    final String as = expr.getString(AS);
    final String from = expr.getString(FROM);
    final boolean inner = expr.getBoolean(INNER, false);
    final Context localContext = getDatabase(expr).map(context::withDatabase).orElse(context);
    final Function<JsonObject, JsonArray> queryFunction = queryFunction(expr, context);

    return expr.getBoolean(UNWIND, false)
        ? stream.flatMapValues(
            v ->
                iterate(
                    with(lookupPublisher(from, queryFunction.apply(v), localContext))
                        .map(result -> createObjectBuilder(v).add(as, result).build())
                        .get()))
        : stream
            .mapValues(
                v ->
                    createObjectBuilder(v)
                        .add(as, lookup(from, queryFunction.apply(v), localContext))
                        .build())
            .filter((k, v) -> !inner || !v.getJsonArray(as).isEmpty());
  }

  private static JsonValue toArray(final JsonValue value) {
    return isArray(value) ? value : a(value);
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

  private static JsonObjectBuilder wrapOuter(
      final JsonObject expression, final String field, final JsonObjectBuilder query) {
    return expression.getBoolean(INNER, false)
        ? query
        : createObjectBuilder()
            .add(
                OR,
                createArrayBuilder()
                    .add(query)
                    .add(createObjectBuilder().add(field, JsonValue.NULL))
                    .add(
                        createObjectBuilder()
                            .add(field, createObjectBuilder().add(EXISTS, false))));
  }
}
