package net.pincette.mongo.streams;

import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.Map;
import java.util.logging.Logger;
import javax.json.JsonObject;
import net.pincette.mongo.Features;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * The context for a pipeline stage.
 *
 * @author Werner Donn\u00e9
 * @since 1.1
 */
public class Context {
  /** The application. */
  public final String app;

  /** The MongoDB database. */
  public final MongoDatabase database;

  /** Extra features for the underlying MongoDB aggregation expression language and JSLT. */
  public final Features features;

  /** A logger in case stages have something to log. */
  public final Logger logger;

  /** The Kafka producer for stages that need to send messages top topics. */
  public final KafkaProducer<String, JsonObject> producer;

  /** Extra stages that will be merged with the built-in stages, which always have precedence. */
  public final Map<String, Stage> stageExtensions;

  /**
   * Writes tracing of the stages to the logger "net.pincette.mongo.streams" at log level <code>INFO
   * </code>.
   */
  public final boolean trace;

  public Context() {
    this(null, null, null, false, null, null, null);
  }

  private Context(
      final String app,
      final MongoDatabase database,
      final KafkaProducer<String, JsonObject> producer,
      final boolean trace,
      final Features features,
      final Map<String, Stage> stageExtensions,
      final Logger logger) {
    this.app = app;
    this.database = database;
    this.producer = producer;
    this.trace = trace;
    this.features = features;
    this.stageExtensions = stageExtensions;
    this.logger = logger;
  }

  public Context withApp(final String app) {
    return new Context(app, database, producer, trace, features, stageExtensions, logger);
  }

  public Context withDatabase(final MongoDatabase database) {
    return new Context(app, database, producer, trace, features, stageExtensions, logger);
  }

  public Context withFeatures(final Features features) {
    return new Context(app, database, producer, trace, features, stageExtensions, logger);
  }

  public Context withLogger(final Logger logger) {
    return new Context(app, database, producer, trace, features, stageExtensions, logger);
  }

  public Context withProducer(final KafkaProducer<String, JsonObject> producer) {
    return new Context(app, database, producer, trace, features, stageExtensions, logger);
  }

  public Context withStageExtensions(final Map<String, Stage> stageExtensions) {
    return new Context(app, database, producer, trace, features, stageExtensions, logger);
  }

  public Context withTrace(final boolean trace) {
    return new Context(app, database, producer, trace, features, stageExtensions, logger);
  }
}
