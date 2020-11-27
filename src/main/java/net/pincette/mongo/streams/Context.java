package net.pincette.mongo.streams;

import com.mongodb.reactivestreams.client.MongoDatabase;
import net.pincette.mongo.Features;

/**
 * The context for a pipeline stage.
 *
 * @author Werner Donn\u00e9
 * @since 1.1
 */
public class Context {
  public final String app;
  public final MongoDatabase database;
  public final Features features;
  public final boolean trace;

  Context(
      final String app,
      final MongoDatabase database,
      final boolean trace,
      final Features features) {
    this.app = app;
    this.database = database;
    this.trace = trace;
    this.features = features;
  }

  Context withDatabase(final MongoDatabase database) {
    return new Context(app, database, trace, features);
  }

  Context withFeatures(final Features features) {
    return new Context(app, database, trace, features);
  }
}
