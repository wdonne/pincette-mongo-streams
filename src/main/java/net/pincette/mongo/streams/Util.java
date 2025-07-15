package net.pincette.mongo.streams;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptySet;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedStage;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.toSet;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToGetForever;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.function.SupplierWithException;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;

/**
 * Some utilities.
 *
 * @author Werner Donné
 */
class Util {
  static final String ID = "_id";
  static final Logger LOGGER = getLogger("net.pincette.mongo.streams");

  static final Duration RETRY = ofSeconds(5);
  private static final String AND = "$and";
  private static final String EQ = "$eq";
  private static final String ON = "on";

  private Util() {}

  private static JsonObject addMatchQueries(final List<Pair<String, JsonValue>> queries) {
    return createObjectBuilder()
        .add(
            AND,
            queries.stream()
                .reduce(
                    createArrayBuilder(), (b, p) -> b.add(eq(p.first, p.second)), (b1, b2) -> b1))
        .build();
  }

  private static JsonObjectBuilder eq(final String field, final JsonValue value) {
    return createObjectBuilder().add(field, createObjectBuilder().add(EQ, value));
  }

  static void exceptionLogger(final Throwable t, final String stage, final Context context) {
    exceptionLogger(t, stage, (String) null, context);
  }

  static void exceptionLogger(
      final Throwable t,
      final String stage,
      final Supplier<String> message,
      final Context context) {
    exceptionLogger(t, stage, message.get(), context);
  }

  static void exceptionLogger(
      final Throwable t, final String stage, final String message, final Context context) {
    ofNullable(context.logger)
        .ifPresent(l -> l.log(SEVERE, t, () -> stage + (message != null ? (", " + message) : "")));
  }

  static String generateKey(final JsonValue key) {
    return toNative(key).toString();
  }

  private static List<Pair<String, JsonValue>> getValues(
      final JsonObject json, final Set<String> fields) {
    return fields.stream()
        .map(field -> getValue(json, toJsonPointer(field)).map(value -> pair(field, value)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .toList();
  }

  static Set<String> matchFields(final JsonObject expression, final String defaultField) {
    return getValue(expression, "/" + ON)
        .map(
            value ->
                isArray(value)
                    ? value.asJsonArray().stream()
                        .filter(JsonUtil::isString)
                        .map(JsonUtil::asString)
                        .map(JsonString::getString)
                        .collect(toSet())
                    : set(asString(value).getString()))
        .orElseGet(() -> defaultField != null ? set(defaultField) : emptySet());
  }

  static Optional<JsonObject> matchQuery(final JsonObject json, final Set<String> fields) {
    return fields.size() == 1
        ? matchQuerySingle(json, fields.iterator().next())
        : matchQueryMultiple(json, fields);
  }

  private static Optional<JsonObject> matchQueryMultiple(
      final JsonObject json, final Set<String> fields) {
    return Optional.of(getValues(json, fields))
        .filter(pairs -> pairs.size() == fields.size())
        .map(Util::addMatchQueries);
  }

  private static Optional<JsonObject> matchQuerySingle(final JsonObject json, final String field) {
    return getValue(json, toJsonPointer(field)).map(value -> eq(field, value).build());
  }

  static <T> CompletionStage<T> tryForever(
      final SupplierWithException<CompletionStage<T>> run,
      final String stage,
      final BooleanSupplier stop,
      final Context context) {
    return tryForever(run, stage, stop, null, context);
  }

  static <T> CompletionStage<T> tryForever(
      final SupplierWithException<CompletionStage<T>> run,
      final String stage,
      final BooleanSupplier stop,
      final Supplier<String> message,
      final Context context) {
    return tryToGetForever(
        () -> stop.getAsBoolean() ? completedStage(null) : run.get(),
        RETRY,
        e -> exceptionLogger(e, stage, message, context));
  }
}
