package net.pincette.mongo.streams;

import static java.util.Collections.emptySet;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.isInt;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.objectValue;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.json.Transform.transform;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Mapper.map;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.allPaths;
import static net.pincette.util.Util.must;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.mongo.Features;
import net.pincette.rs.streams.Message;

/**
 * The <code>$project</code> operator.
 *
 * @author Werner Donné
 */
class Project {
  private static final String ID = "_id";
  private static final JsonValue REMOVE = createValue("REMOVE");
  private static final String REMOVE_VAR = "$$REMOVE";

  private Project() {}

  private static boolean exclude(final JsonValue value) {
    return FALSE.equals(value) || (isInt(value) && asInt(value) == 0);
  }

  private static Set<String> expand(final Set<String> include) {
    return include.stream()
        .flatMap(i -> allPaths(i, "."))
        .map(Project::removeExpression)
        .collect(toSet());
  }

  private static Optional<String> field(
      final String key, final JsonValue value, final Predicate<JsonValue> predicate) {
    return objectValue(value)
        .filter(j -> j.size() == 1)
        .map(j -> j.entrySet().iterator().next())
        .map(e -> field(key + "." + e.getKey(), e.getValue(), predicate))
        .orElseGet(() -> Optional.of(value).filter(predicate).map(v -> key));
  }

  private static Set<String> findFields(
      final JsonObject json, final Predicate<JsonValue> predicate) {
    return json.entrySet().stream()
        .map(e -> field(e.getKey(), e.getValue(), predicate))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toSet());
  }

  private static boolean include(final JsonValue value) {
    return TRUE.equals(value) || (isInt(value) && asInt(value) == 1);
  }

  private static JsonObject project(
      final JsonObject json,
      final Set<String> include,
      final Set<String> exclude,
      final Map<String, Function<JsonObject, JsonValue>> updates) {
    final Set<String> allIncludes = expand(union(include, updates.keySet()));

    return transform(
        json,
        new Transformer(
                e ->
                    exclude.contains(e.path)
                        || (!ID.equals(e.path)
                            && !allIncludes.isEmpty()
                            && !shouldInclude(e.path, allIncludes)),
                e -> empty())
            .thenApply(
                new Transformer(
                    e -> updates.containsKey(e.path),
                    e ->
                        Optional.of(updates.get(e.path))
                            .map(function -> function.apply(json))
                            .filter(value -> !value.equals(REMOVE))
                            .map(value -> new JsonEntry(e.path, value)))));
  }

  private static String removeExpression(final String path) {
    return Optional.of(path.indexOf(".$"))
        .filter(i -> i != -1)
        .map(i -> path.substring(0, i))
        .orElse(path);
  }

  private static boolean shouldInclude(final String path, final Set<String> includes) {
    return allPaths(path, ".").anyMatch(includes::contains);
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();

    must(!expr.isEmpty() && ofNullable(expr.get(ID)).map(Project::exclude).orElse(true));

    final Set<String> exclude =
        union(findFields(expr, Project::exclude), expr.containsKey(ID) ? set(ID) : emptySet());
    final Set<String> include = findFields(expr, Project::include);
    final Map<String, Function<JsonObject, JsonValue>> updates =
        updates(expr, findFields(expr, Project::update), context.features);

    must(
        exclude.isEmpty()
            || (exclude.size() == 1 && exclude.contains(ID))
            || (include.isEmpty() && updates.isEmpty()));

    return map(m -> m.withValue(project(m.value, include, exclude, updates)));
  }

  private static boolean update(final JsonValue value) {
    return !include(value) && !exclude(value);
  }

  private static Map<String, Function<JsonObject, JsonValue>> updates(
      final JsonObject expression, final Set<String> update, final Features features) {
    return update.stream()
        .map(Project::removeExpression)
        .collect(
            toMap(
                u -> u,
                u ->
                    function(
                        ofNullable(expression.get(u))
                            .orElseGet(() -> expression.getValue(toJsonPointer(u))),
                        map(pair(REMOVE_VAR, REMOVE)),
                        features)));
  }
}
