package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Logger.getGlobal;
import static net.pincette.mongo.streams.Pipeline.create;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Reducer.forEach;
import static net.pincette.rs.Reducer.forEachJoin;
import static net.pincette.rs.Util.asList;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.json.JsonArray;
import javax.json.JsonObject;
import net.pincette.rs.Source;
import net.pincette.rs.streams.Message;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class Base {
  static final String APP = "pincette-mongo-streams";
  static final String ID = "_id";
  static Resources resources;

  @AfterAll
  static void after() {
    cleanUpCollections();
    resources.close();
  }

  @BeforeAll
  static void before() {
    resources = new Resources();
    cleanUpCollections();
  }

  private static void cleanUpCollections() {
    forEachJoin(
        with(toFlowPublisher(resources.database.listCollectionNames()))
            .filter(name -> name.startsWith(APP))
            .get(),
        name -> forEach(toFlowPublisher(resources.database.getCollection(name).drop()), v -> {}));
  }

  static Message<String, JsonObject> inputMessage(final JsonObject message) {
    return message(
        ofNullable(message.getString(ID, null)).orElseGet(() -> randomUUID().toString()), message);
  }

  static List<Message<String, JsonObject>> inputMessages(final List<JsonObject> messages) {
    return messages.stream().map(Base::inputMessage).toList();
  }

  protected void drop(final String collection) {
    forEachJoin(toFlowPublisher(resources.database.getCollection(collection).drop()), v -> {});
  }

  protected List<Message<String, JsonObject>> runTest(
      final JsonArray pipeline, final List<JsonObject> messages) {
    return runTest(pipeline, messages, null);
  }

  protected List<Message<String, JsonObject>> runTest(
      final JsonArray pipeline,
      final List<JsonObject> messages,
      final BiFunction<String, Message<String, JsonObject>, CompletionStage<Boolean>> producer) {
    return runTest(pipeline, () -> Source.of(inputMessages(messages)), producer);
  }

  protected List<Message<String, JsonObject>> runTest(
      final JsonArray pipeline,
      final Supplier<Publisher<Message<String, JsonObject>>> messages,
      final BiFunction<String, Message<String, JsonObject>, CompletionStage<Boolean>> producer) {
    final Processor<Message<String, JsonObject>, Message<String, JsonObject>> pipe =
        create(
            pipeline,
            new Context()
                .withApp(APP)
                .withDatabase(resources.database)
                .withLogger(getGlobal())
                .withStageExtensions(map(pair("$wait", (expr, ctx) -> Wait.stage(expr))))
                .withProducer(producer != null ? producer : (t, m) -> completedFuture(true)));

    messages.get().subscribe(pipe);

    return asList(pipe);
  }
}
