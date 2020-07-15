package net.pincette.mongo.streams;

import com.mongodb.reactivestreams.client.MongoDatabase;
import net.pincette.mongo.Features;

class Context {
  final String app;
  final MongoDatabase database;
  final Features features;
  final boolean trace;

  Context(final String app, final MongoDatabase database, final boolean trace, final Features features) {
    this.app = app;
    this.database = database;
    this.trace = trace;
    this.features = features;
  }
}
