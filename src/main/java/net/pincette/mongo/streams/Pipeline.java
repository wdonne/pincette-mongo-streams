package net.pincette.mongo.streams;

import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static java.util.logging.Level.INFO;
import static java.util.logging.Logger.getLogger;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;

import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Features;
import org.apache.kafka.streams.kstream.KStream;

/**
 * With this class you can build Kafka streams using MongoDB aggregation pipeline descriptions. All
 * Kafka streams are expressed in terms of <code>javax.json.JsonObject</code>, so you need a
 * serialiser/deserialiser for that. A candidate is <code>net.pincette.jes.util.JsonSerde</code>,
 * which uses compressed CBOR. Only pipeline stages that have a meaning for infinite streams are
 * supported. This is the list:
 *
 * <dl>
 *   <dt><a
 *       href="https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/">$addFields</a>
 *   <dd>Supports the expressions defined in <code>{@link net.pincette.mongo.Expression}.</code>
 *   <dt><a
 *       href="https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/">$bucket</a>
 *   <dd>This operator is implemented in terms of the <code>$group</code> and <code>$switch</code>
 *       operators, so their constraints apply here. The extension field <code>_collection</code> is
 *       also available.
 *   <dt><a href="https://docs.mongodb.com/manual/reference/operator/aggregation/count/">$count</a>
 *   <dt>$delete
 *   <dd>This extension operator has a specification with the mandatory fields <code>from</code> and
 *       <code>on</code>. The former is the name of a MongoDB collection. The latter is either a
 *       string or a non-empty array of strings. It represents fields in the incoming JSON object.
 *       The operator deletes records from the collection for which the given fields have the same
 *       values as for the incoming JSON object. The output of the operator is the incoming JSON
 *       object.
 *   <dt><a href="https://docs.mongodb.com/manual/reference/operator/aggregation/group/">$group</a>
 *   <dd>Because streams are infinite there are a few deviations for this operator. The accumulation
 *       operators <code>$first</code>, <code>$last</code> and <code>$stdDevSamp</code> don't exist.
 *       The generated Kafka stream will also emit a message each time the value of the grouping
 *       changes. Therefore, the <code>$stdDevPop</code> operator represents the running standard
 *       deviation. The accumulation operators support the expressions defined in <code>
 *       {@link net.pincette.mongo.Expression}</code>. The <code>$group</code> stage doesn't use
 *       KTables. Instead it uses a backing collection in MongoDB. You can specify one with the
 *       extension property <code>_collection</code>. Otherwise a unique collection name will be
 *       used to create one. The name is prefixed with the application name. A reason to specify a
 *       collection, for example, is when your grouping key is time-based and won't get any new
 *       values after some period. You can then set a <a
 *       href="https://docs.mongodb.com/manual/core/index-ttl/">TTL index</a> to get rid of old
 *       records quickly.
 *   <dt>$jslt
 *   <dd>This extension operator transforms the incoming message with a <a
 *       href="https://github.com/schibsted/jslt">JSLT</a> script. Its specification should be a
 *       string. If it starts with "resource:/" the script will be loaded as a class path resource,
 *       otherwise it is interpreted as a filename. If the transformation changes or adds the <code>
 *       _id</code> field then that will become the key of the outgoing message.
 *   <dt><a
 *       href="https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/">$lookup</a>
 *   <dd>The extra optional boolean field <code>inner</code> is available to make this pipeline
 *       stage behave like an inner join instead of an outer left join, which is the default.
 *   <dt><a href="https://docs.mongodb.com/manual/reference/operator/aggregation/match/">$match</a>
 *   <dd>Supports the expressions defined in <code>{@link net.pincette.mongo.Match}.</code>
 *   <dt><a href="https://docs.mongodb.com/manual/reference/operator/aggregation/merge/">$merge</a>
 *   <dd>Pipelines for the <code>whenMatched</code> field are currently not supported. The <code>
 *       into</code> field can only be the name of a collection. The database is always the one
 *       given to the pipeline. The output of the stream is whatever has been updated to or taken
 *       from the MongoDB collection.
 *   <dt><a href="https://docs.mongodb.com/manual/reference/operator/aggregation/out/">$out</a>
 *   <dd>Because streams are infinite this operator behaves like <code>$merge</code> with the
 *       following settings:
 *       <pre>       {
 *         "into": "&lt;output-collection&gt;",
 *         "on": "_id",
 *         "whenMatched": "replace",
 *         "whenNotMatched": "insert"
 *       }</pre>
 *   <dt>$probe
 *   <dd>With this extension operator you can monitor the throughput anywhere in a pipeline. The
 *       specification is an object with the fields <code>name</code> and <code>topic</code>, which
 *       is the name of a Kafka topic. It will write messages to that topic with the fields <code>
 *       name</code>, <code>minute</code> and <code>count</code>, which represents the number of
 *       messages it has seen in that minute. Note that if your pipeline is running on multiple
 *       topic partitions you should group the messages on the specified topic by the name and
 *       minute and sum the count. That is because every instance of the pipeline only sees the
 *       messages that pass on the partitions that are assigned to it.
 *   <dt><a
 *       href="https://docs.mongodb.com/manual/reference/operator/aggregation/project/">$project</a>
 *   <dd>Supports the expressions defined in <code>{@link net.pincette.mongo.Expression}.</code>
 *   <dt><a
 *       href="https://docs.mongodb.com/manual/reference/operator/aggregation/redact/">$redact</a>
 *   <dd>Supports the expressions defined in <code>{@link net.pincette.mongo.Expression}.</code>
 *   <dt><a
 *       href="https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/">$replaceRoot</a>
 *   <dd>Supports the expressions defined in <code>{@link net.pincette.mongo.Expression}.</code>
 *   <dt><a
 *       href="https://docs.mongodb.com/manual/reference/operator/aggregation/replaceWith/">$replaceWith</a>
 *   <dd>Supports the expressions defined in <code>{@link net.pincette.mongo.Expression}.</code>
 *   <dt><a href="https://docs.mongodb.com/manual/reference/operator/aggregation/set/">$set</a>
 *   <dd>Supports the expressions defined in <code>{@link net.pincette.mongo.Expression}.</code>
 *   <dt>$setKey
 *   <dd>With this extension operator you can change the Kafka key of the message without changing
 *       the message itself. The operator expects an expression, the result of which will be
 *       converted to a string. Supports the expressions defined in <code>
 *       {@link net.pincette.mongo.Expression}.</code>
 *   <dt>$trace
 *   <dd>This extension operator writes all JSON objects that pass through it to the Java logger
 *       "net.pinctte.mongo.streams" with level <code>INFO</code>. That is, when the operator
 *       doesn't have an expression (set to <code>null</code>). If you give it an expression, its
 *       result will be written to the logger. This can be used for pipeline debugging. Supports the
 *       expressions defined in <code>{@link net.pincette.mongo.Expression}.</code>
 *   <dt><a href="https://docs.mongodb.com/manual/reference/operator/aggregation/unset/">$unset</a>
 *   <dt><a
 *       href="https://docs.mongodb.com/manual/reference/operator/aggregation/unwind/">$unwind</a>
 *   <dd>The boolean extension option <code>newIds</code> will cause UUIDs to be generated for for
 *       the output documents if the given array was not absent or empty.
 * </dl>
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 * @see net.pincette.mongo.Match
 * @see net.pincette.mongo.Expression
 */
public class Pipeline {
  private static final String ADD_FIELDS = "$addFields";
  private static final String BUCKET = "$bucket";
  private static final String COUNT = "$count";
  private static final String DELETE = "$delete";
  private static final String GROUP = "$group";
  private static final String JSLT = "$jslt";
  private static final String LOOKUP = "$lookup";
  private static final String MATCH = "$match";
  private static final String MERGE = "$merge";
  private static final String OUT = "$out";
  private static final String PROBE = "$probe";
  private static final String PROJECT = "$project";
  private static final String REDACT = "$redact";
  private static final String REPLACE_ROOT = "$replaceRoot";
  private static final String REPLACE_WITH = "$replaceWith";
  private static final String SET = "$set";
  private static final String SET_KEY = "$setKey";
  private static final String TRACE = "$trace";
  private static final String UNSET = "$unset";
  private static final String UNWIND = "$unwind";
  private static final Logger logger = getLogger("net.pincette.mongo.streams");
  private static final Map<String, Stage> stages =
      map(
          pair(ADD_FIELDS, AddFields::stage),
          pair(BUCKET, Bucket::stage),
          pair(COUNT, Count::stage),
          pair(DELETE, Delete::stage),
          pair(GROUP, Group::stage),
          pair(JSLT, Jslt::stage),
          pair(LOOKUP, Lookup::stage),
          pair(MATCH, Match::stage),
          pair(MERGE, Merge::stage),
          pair(OUT, Out::stage),
          pair(PROBE, (st, ex, ctx) -> Probe.stage(st, ex)),
          pair(PROJECT, Project::stage),
          pair(REDACT, Redact::stage),
          pair(REPLACE_ROOT, ReplaceRoot::stage),
          pair(REPLACE_WITH, ReplaceRoot::stage),
          pair(SET, AddFields::stage),
          pair(SET_KEY, SetKey::stage),
          pair(TRACE, Trace::stage),
          pair(UNSET, Unset::stage),
          pair(UNWIND, (st, ex, ctx) -> Unwind.stage(st, ex)));

  private Pipeline() {}

  /**
   * Appends an aggregation <code>pipeline</code> to a given <code>stream</code>, resulting in a new
   * stream. Pipeline stages that are not recognised are ignored.
   *
   * @param app the application.
   * @param stream the given Kafka stream.
   * @param pipeline the aggregation pipeline.
   * @param database the MongoDB database.
   * @return The new Kafka stream.
   * @since 1.0
   */
  public static KStream<String, JsonObject> create(
      final String app,
      final KStream<String, JsonObject> stream,
      final JsonArray pipeline,
      final MongoDatabase database) {
    return create(app, stream, pipeline, database, false);
  }

  /**
   * Appends an aggregation <code>pipeline</code> to a given <code>stream</code>, resulting in a new
   * stream. Pipeline stages that are not recognised are ignored.
   *
   * @param app the application.
   * @param stream the given Kafka stream.
   * @param pipeline the aggregation pipeline.
   * @param database the MongoDB database.
   * @param trace writes tracing of the stages to the logger "net.pincette.mongo.streams" at log
   *     level <code>INFO</code>.
   * @return The new Kafka stream.
   * @since 1.0
   */
  public static KStream<String, JsonObject> create(
      final String app,
      final KStream<String, JsonObject> stream,
      final JsonArray pipeline,
      final MongoDatabase database,
      final boolean trace) {
    return create(app, stream, pipeline, database, trace, null);
  }

  /**
   * Appends an aggregation <code>pipeline</code> to a given <code>stream</code>, resulting in a new
   * stream. Pipeline stages that are not recognised are ignored.
   *
   * @param app the application.
   * @param stream the given Kafka stream.
   * @param pipeline the aggregation pipeline.
   * @param database the MongoDB database.
   * @param trace writes tracing of the stages to the logger "net.pincette.mongo.streams" at log
   *     level <code>INFO</code>.
   * @param features extra features for the underlying MongoDB aggregation expression language. It
   *     may be <code>null</code>.
   * @return The new Kafka stream.
   * @since 1.0.1
   */
  public static KStream<String, JsonObject> create(
      final String app,
      final KStream<String, JsonObject> stream,
      final JsonArray pipeline,
      final MongoDatabase database,
      final boolean trace,
      final Features features) {
    final Context context = new Context(app, database, trace, features);

    return pipeline.stream()
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(
            json ->
                name(json)
                    .flatMap(
                        name ->
                            ofNullable(stages.get(name))
                                .map(
                                    stage ->
                                        pair(
                                            trace ? wrapProfile(stage, name) : stage,
                                            json.getValue("/" + name)))))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .reduce(stream, (s, p) -> p.first.apply(s, p.second, context), (s1, s2) -> s1);
  }

  private static KStream<String, JsonObject> markEnd(
      final long[] start,
      final KStream<String, JsonObject> stream,
      final String name,
      final JsonValue expression) {
    return stream.mapValues(
        v ->
            SideEffect.<JsonObject>run(
                    () ->
                        logger.log(
                            INFO,
                            "{0} with expression {1} took {2}ms",
                            new Object[] {
                              name, string(expression), now().toEpochMilli() - start[0]
                            }))
                .andThenGet(() -> v));
  }

  private static KStream<String, JsonObject> markStart(
      final long[] start, final KStream<String, JsonObject> stream) {
    return stream.mapValues(
        v -> SideEffect.<JsonObject>run(() -> start[0] = now().toEpochMilli()).andThenGet(() -> v));
  }

  private static Optional<String> name(final JsonObject json) {
    return Optional.of(json.keySet())
        .filter(keys -> keys.size() == 1)
        .map(keys -> keys.iterator().next());
  }

  private static Stage wrapProfile(final Stage stage, final String name) {
    final long[] start = new long[1];

    return (stream, expression, context) ->
        markEnd(
            start, stage.apply(markStart(start, stream), expression, context), name, expression);
  }
}
