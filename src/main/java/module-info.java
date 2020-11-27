module net.pincette.mongo.streams {
  requires java.json;
  requires net.pincette.json;
  requires kafka.streams;
  requires org.mongodb.driver.reactivestreams;
  requires net.pincette.common;
  requires net.pincette.mongo;
  requires java.logging;
  requires org.mongodb.bson;
  requires org.mongodb.driver.core;
  requires io.netty.handler;
  requires async.http.client;
  requires net.pincette.rs;
  requires org.reactivestreams;
  exports net.pincette.mongo.streams;
}
