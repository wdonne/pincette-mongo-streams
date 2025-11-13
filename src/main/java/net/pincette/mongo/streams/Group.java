package net.pincette.mongo.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.security.MessageDigest.getInstance;
import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.INFO;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createDiff;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isBoolean;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.mongo.BsonUtil.isoDateJson;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.mongo.Util.compare;
import static net.pincette.mongo.streams.Util.ID;
import static net.pincette.mongo.streams.Util.TIMESTAMP;
import static net.pincette.mongo.streams.Util.generateKey;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.toHex;
import static net.pincette.util.Util.tryToGetRethrow;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Features;
import net.pincette.rs.Mapper;
import net.pincette.rs.streams.Message;
import net.pincette.util.Pair;
import net.pincette.util.Util.GeneralException;
import org.bson.Document;

/**
 * The <code>$group</code> operator.
 *
 * @author Werner Donné
 */
class Group {
  private static final String ADD_TO_SET = "$addToSet";
  private static final JsonValue ALL = createValue("all");
  private static final String AVG = "avg";
  private static final String AVG_OP = "$avg";
  private static final String COLLECTION = "_collection";
  private static final String COUNT = "count";
  private static final String COUNT_OP = "$count";
  private static final String LAST = "$last";
  private static final String MAX = "$max";
  private static final String MERGE_OBJECTS = "$mergeObjects";
  private static final String MIN = "$min";
  private static final String N = "n";
  private static final String PUSH = "$push";
  private static final String SIGMA = "sigma";
  private static final String STD_DEV_POP = "$stdDevPop";
  private static final String SUM = "$sum";
  private static final String S1 = "s1";
  private static final String S2 = "s2";
  private static final String TOTAL = "total";
  private static final Map<String, Implementation> aggregators =
      map(
          pair(ADD_TO_SET, Group::addToSet),
          pair(AVG_OP, Group::avg),
          pair(COUNT_OP, Group::count),
          pair(LAST, Group::last),
          pair(MAX, Group::max),
          pair(MERGE_OBJECTS, Group::mergeObjects),
          pair(MIN, Group::min),
          pair(PUSH, Group::push),
          pair(STD_DEV_POP, Group::stdDevPop),
          pair(SUM, Group::sum));
  private static final MessageDigest digester =
      tryToGetRethrow(() -> getInstance("MD5")).orElse(null);
  private static final Logger logger = getLogger("net.pincette.mongo.streams");
  private static final Map<String, Selector> selectors =
      map(pair(AVG_OP, Group::avgSelect), pair(STD_DEV_POP, Group::stdDevPopSelect));

  private Group() {}

  private static JsonObject addTimestamp(final JsonObject json) {
    return createObjectBuilder(json).add(TIMESTAMP, isoDateJson(now())).build();
  }

  private static Operator addToSet(final JsonValue expression, final Features features) {
    final Function<JsonObject, JsonValue> function = expression(expression, features);

    return (current, json) ->
        Optional.of(function.apply(json))
            .filter(value -> !NULL.equals(value))
            .map(
                value ->
                    concat(
                            current != null
                                ? current.asJsonArray().stream()
                                : Stream.<JsonValue>empty(),
                            Stream.of(value))
                        .collect(toSet())
                        .stream()
                        .sorted(net.pincette.mongo.Util::compare)
                        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
                        .build())
            .orElse((JsonArray) current);
  }

  private static JsonObject aggregate(
      final JsonObject current,
      final JsonValue key,
      final JsonObject json,
      final Map<String, Operator> operators) {
    return operators.entrySet().stream()
        .map(e -> pair(e.getKey(), e.getValue().apply(current.get(e.getKey()), json)))
        .reduce(
            createObjectBuilder(current).add(ID, key),
            (b, p) -> b.add(p.first, p.second),
            (b1, b2) -> b1)
        .build();
  }

  private static BiFunction<JsonValue, JsonObject, CompletionStage<JsonObject>> aggregator(
      final JsonObject expression,
      final MongoCollection<Document> collection,
      final Context context) {
    final Map<String, Operator> operators = operatorsPerField(expression, context.features);
    final Map<String, Selector> selectors = selectorsPerField(expression);

    return (key, json) ->
        findOne(collection, eq(ID, toNative(key)))
            .thenApply(current -> current.orElseGet(JsonUtil::emptyObject))
            .thenApply(Group::removeTimestamp)
            .thenComposeAsync(
                current ->
                    Optional.of(aggregate(current, key, json, operators))
                        .filter(
                            newCurrent -> !createDiff(current, newCurrent).toJsonArray().isEmpty())
                        .map(
                            newCurrent ->
                                update(collection, addTimestamp(newCurrent))
                                    .thenApply(result -> must(result, r -> r))
                                    .thenApply(result -> select(newCurrent, selectors)))
                        .orElse(completedFuture(null)));
  }

  private static Operator avg(final JsonValue expression, final Features features) {
    return numbers(expression, Group::avg, features);
  }

  private static JsonValue avg(final JsonObject current, final double value) {
    final long count = current != null ? (asNumber(current.get(COUNT)).longValue() + 1) : 1;
    final double total = (current != null ? asNumber(current.get(TOTAL)).doubleValue() : 0) + value;

    return createObjectBuilder()
        .add(COUNT, count)
        .add(TOTAL, total)
        .add(AVG, total / count)
        .build();
  }

  private static JsonValue avgSelect(final JsonValue value) {
    return value.asJsonObject().get(AVG);
  }

  private static Operator count(final JsonValue expression, final Features features) {
    must(
        expression,
        e -> isObject(e) && e.asJsonObject().isEmpty(),
        e -> {
          throw new GeneralException(
              "The value of the $count aggregation expression is "
                  + string(e)
                  + " instead of the empty object.");
        });

    return sum(createValue(1), features);
  }

  private static String digest(final JsonValue expression) {
    return valueOf(toHex(digester.digest(string(expression).getBytes(UTF_8))));
  }

  private static Function<JsonObject, JsonValue> expression(
      final JsonValue expression, final Features features) {
    return isExpressionObject(expression)
        ? expressionObject(expression.asJsonObject(), features)
        : function(expression, features);
  }

  private static Function<JsonObject, JsonValue> expressionObject(
      final JsonObject expression, final Features features) {
    final Map<String, Function<JsonObject, JsonValue>> nested =
        expression.entrySet().stream()
            .collect(toMap(Entry::getKey, e -> expression(e.getValue(), features)));

    return json ->
        nested.entrySet().stream()
            .reduce(
                createObjectBuilder(),
                (b, e) -> b.add(e.getKey(), e.getValue().apply(json)),
                (b1, b2) -> b1)
            .build();
  }

  private static <T> Map<String, T> functionsPerField(
      final JsonObject expression, final BiFunction<String, JsonObject, T> get) {
    return operators(expression)
        .map(
            pair ->
                operator(pair.second)
                    .map(op -> get.apply(op, pair.second))
                    .map(function -> pair(pair.first, function)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toMap(p -> p.first, p -> p.second));
  }

  private static boolean hasId(final JsonStructure json) {
    return getValue(json, "/" + ID).map(val -> !isArray(val)).orElse(true);
  }

  private static boolean isExpressionObject(final JsonValue expression) {
    return isObject(expression)
        && expression.asJsonObject().keySet().stream().anyMatch(key -> !key.startsWith("$"));
  }

  private static boolean isIdentifier(final JsonValue value) {
    return isString(value) && asString(value).getString().startsWith("$");
  }

  private static boolean isLiteral(final JsonValue value) {
    return NULL.equals(value)
        || isBoolean(value)
        || (isString(value) && !isIdentifier(value))
        || isNumber(value);
  }

  private static Operator last(final JsonValue expression, final Features features) {
    final Function<JsonObject, JsonValue> function = expression(expression, features);

    return (current, json) -> Optional.of(function.apply(json)).orElse(current);
  }

  private static Operator max(final JsonValue expression, final Features features) {
    return minMax(expression, (current, value) -> compare(value, current) > 0, features);
  }

  private static Operator mergeObjects(final JsonValue expression, final Features features) {
    final Function<JsonObject, JsonValue> function = expression(expression, features);

    return (current, json) ->
        copy(
                Optional.of(function.apply(json))
                    .filter(JsonUtil::isObject)
                    .map(JsonValue::asJsonObject)
                    .orElseGet(JsonUtil::emptyObject),
                current != null
                    ? createObjectBuilder(current.asJsonObject())
                    : createObjectBuilder())
            .build();
  }

  private static Operator min(final JsonValue expression, final Features features) {
    return minMax(expression, (current, value) -> compare(value, current) < 0, features);
  }

  private static Operator minMax(
      final JsonValue expression,
      final BiPredicate<JsonValue, JsonValue> predicate,
      final Features features) {
    final Function<JsonObject, JsonValue> function = expression(expression, features);

    return (current, json) ->
        Optional.of(function.apply(json))
            .filter(value -> current == null || predicate.test(current, value))
            .orElse(current);
  }

  private static Operator numbers(
      final JsonValue expression,
      final BiFunction<JsonObject, Double, JsonValue> op,
      final Features features) {
    final Function<JsonObject, JsonValue> function = expression(expression, features);

    return (current, json) ->
        Optional.of(function.apply(json))
            .filter(JsonUtil::isNumber)
            .map(JsonUtil::asNumber)
            .map(JsonNumber::doubleValue)
            .map(value -> op.apply(current != null ? current.asJsonObject() : null, value))
            .orElse(current);
  }

  private static Optional<String> operator(final JsonValue value) {
    return Optional.of(value)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(JsonObject::keySet)
        .filter(keys -> keys.size() == 1)
        .map(keys -> keys.iterator().next());
  }

  private static Stream<Pair<String, JsonObject>> operators(final JsonObject expression) {
    return expression.entrySet().stream()
        .filter(e -> !e.getKey().equals(ID) && isObject(e.getValue()))
        .map(e -> pair(e.getKey(), e.getValue().asJsonObject()));
  }

  private static Map<String, Operator> operatorsPerField(
      final JsonObject expression, final Features features) {
    return functionsPerField(
        expression,
        (op, expr) ->
            ofNullable(aggregators.get(op))
                .map(aggregator -> aggregator.apply(expr.get(op), features))
                .orElse(null));
  }

  private static Operator push(final JsonValue expression, final Features features) {
    final Function<JsonObject, JsonValue> function = expression(expression, features);

    return (current, json) ->
        Optional.of(function.apply(json))
            .map(
                value ->
                    (current != null
                            ? createArrayBuilder(current.asJsonArray())
                            : createArrayBuilder())
                        .add(value)
                        .build())
            .orElse((JsonArray) current);
  }

  private static JsonObject removeTimestamp(final JsonObject json) {
    return createObjectBuilder(json).remove(TIMESTAMP).build();
  }

  private static JsonObject select(
      final JsonObject current, final Map<String, Selector> selectors) {
    return current.entrySet().stream()
        .map(
            e ->
                pair(
                    e.getKey(),
                    ID.equals(e.getKey()) && ALL.equals(e.getValue()) ? NULL : e.getValue()))
        .reduce(
            createObjectBuilder(),
            (b, p) ->
                b.add(
                    p.first,
                    ofNullable(selectors.get(p.first))
                        .map(selector -> selector.apply(p.second))
                        .orElse(p.second)),
            (b1, b2) -> b1)
        .build();
  }

  private static Map<String, Selector> selectorsPerField(final JsonObject expression) {
    return functionsPerField(expression, (op, expr) -> selectors.get(op));
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();
    final String collection =
        ofNullable(expr.getString(COLLECTION, null))
            .orElseGet(() -> context.app + "-" + digest(expression));
    final BiFunction<JsonValue, JsonObject, CompletionStage<JsonObject>> aggregator =
        aggregator(expr, context.database.getCollection(collection), context);
    final JsonValue groupExpression = expr.getValue("/" + ID);
    final Function<JsonObject, JsonValue> key =
        isLiteral(groupExpression) ? (json -> ALL) : expression(groupExpression, context.features);

    if (context.trace) {
      logger.log(INFO, "$group collection {0}", collection);
    }

    return pipe(Mapper.<Message<String, JsonObject>, Pair<JsonValue, JsonObject>>map(
            m -> pair(key.apply(m.value), m.value)))
        .then(
            mapAsyncSequential(
                pair ->
                    aggregator.apply(pair.first, pair.second).thenApply(v -> pair(pair.first, v))))
        .then(filter(pair -> pair.second != null && hasId(pair.second)))
        .then(map(pair -> message(generateKey(pair.first), pair.second)));
  }

  private static Operator stdDevPop(final JsonValue expression, final Features features) {
    return numbers(expression, Group::stdDevPop, features);
  }

  private static JsonValue stdDevPop(final JsonObject current, final double value) {
    final long n = current != null ? (asNumber(current.get(N)).longValue() + 1) : 1;
    final double s1 = (current != null ? asNumber(current.get(S1)).doubleValue() : 0) + value;
    final double s2 =
        (current != null ? asNumber(current.get(S2)).doubleValue() : 0) + pow(value, 2);

    return createObjectBuilder()
        .add(N, n)
        .add(S1, s1)
        .add(S2, s2)
        .add(SIGMA, sqrt(n * s2 - pow(s1, 2)) / n)
        .build();
  }

  private static JsonValue stdDevPopSelect(final JsonValue value) {
    return value.asJsonObject().get(N);
  }

  private static Operator sum(final JsonValue expression, final Features features) {
    final Function<JsonObject, JsonValue> function = expression(expression, features);

    return (current, json) ->
        Optional.of(function.apply(json))
            .filter(JsonUtil::isNumber)
            .map(JsonUtil::asNumber)
            .map(JsonNumber::doubleValue)
            .map(
                value ->
                    createValue((current != null ? asNumber(current).doubleValue() : 0) + value))
            .map(value -> isLong(value) ? createValue(asLong(value)) : value)
            .orElse(current);
  }

  interface Implementation extends BiFunction<JsonValue, Features, Operator> {}

  interface Operator extends BiFunction<JsonValue, JsonObject, JsonValue> {}

  interface Selector extends UnaryOperator<JsonValue> {}
}
