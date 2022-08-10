module net.pincette.mongo.streams {
  requires java.json;
  requires net.pincette.json;
  requires org.mongodb.driver.reactivestreams;
  requires net.pincette.common;
  requires net.pincette.mongo;
  requires java.logging;
  requires org.mongodb.bson;
  requires org.mongodb.driver.core;
  requires io.netty.handler;
  requires net.pincette.rs;
  requires net.pincette.rs.json;
  requires net.pincette.rs.streams;
  requires org.reactivestreams;
  requires net.pincette.netty.http;
  requires io.netty.buffer;
  requires io.netty.codec.http;

  exports net.pincette.mongo.streams;
}
