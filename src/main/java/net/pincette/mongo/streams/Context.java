package net.pincette.mongo.streams;

import com.mongodb.reactivestreams.client.MongoDatabase;

class Context {
  public final String app;
  public final MongoDatabase database;
  public final boolean trace;

  Context(final String app, final MongoDatabase database, final boolean trace) {
    this.app = app;
    this.database = database;
    this.trace = trace;
  }
}
