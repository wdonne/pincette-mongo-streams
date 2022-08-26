package net.pincette.mongo.streams;

import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.objects;
import static net.pincette.netty.http.TestUtil.resourceHandler;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.netty.http.HttpServer;
import net.pincette.rs.streams.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestHttp extends Base {
  private HttpServer server;

  private static List<Message<String, JsonObject>> output(final String name) {
    return read(resource(name, "output.json")).stream()
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(o -> message(o.getString(ID, ""), o))
        .collect(toList());
  }

  private static JsonArray read(final String resource) {
    return createReader(TestHttp.class.getResourceAsStream(resource)).readArray();
  }

  private static String resource(final String test, final String name) {
    return "/" + test + "/" + name;
  }

  @Test
  @DisplayName("$http 1")
  void http1() {
    runHttpTest("http1");
  }

  @Test
  @DisplayName("$http 2")
  void http2() {
    runHttpTest("http2");
  }

  @Test
  @DisplayName("$http 3")
  void http3() {
    runHttpTest("http3");
  }

  @Test
  @DisplayName("$http 4")
  void http4() {
    runHttpTest("http4");
  }

  @Test
  @DisplayName("$http 5")
  void http5() {
    runHttpTest("http5");
  }

  @Test
  @DisplayName("$http 5 recover")
  void http5Recover() {
    stopServer();
    runAsyncAfter(this::startServer, ofSeconds(20));
    runHttpTest("http5");
  }

  private void runHttpTest(final String name) {
    assertEquals(
        output(name),
        runTest(
            read(resource(name, "pipeline.json")),
            objects(read(resource(name, "input.json"))).collect(toList())));
  }

  @BeforeEach
  void startServer() {
    server = new HttpServer(9000, resourceHandler(path -> "application/json"));
    server.run();
  }

  @AfterEach
  void stopServer() {
    server.close();
  }
}
