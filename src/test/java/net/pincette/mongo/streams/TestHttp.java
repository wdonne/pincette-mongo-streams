package net.pincette.mongo.streams;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.time.Duration.ofSeconds;
import static net.pincette.io.StreamConnector.copy;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.objects;
import static net.pincette.netty.http.Dispatcher.when;
import static net.pincette.netty.http.HttpServer.accumulate;
import static net.pincette.netty.http.TestUtil.resourceHandler;
import static net.pincette.netty.http.Util.simpleResponse;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static net.pincette.util.Util.initLogging;
import static net.pincette.util.Util.tryToDoRethrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.RequestHandlerAccumulated;
import net.pincette.rs.Source;
import net.pincette.rs.streams.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestHttp extends Base {
  private HttpServer server;

  @BeforeAll
  static void beforeAll() {
    initLogging();
  }

  private static List<Message<String, JsonObject>> output(final String name) {
    return read(resource(name, "output.json")).stream()
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(o -> message(o.getString(ID, ""), o))
        .toList();
  }

  private static RequestHandlerAccumulated postHandler() {
    return (request, requestBody, response) -> {
      assertEquals(
          request.headers().get("desired-content-type"), request.headers().get(CONTENT_TYPE));

      final ByteArrayOutputStream out = new ByteArrayOutputStream();

      tryToDoRethrow(() -> copy(requestBody, out));

      return simpleResponse(
          response,
          OK,
          request.headers().get(CONTENT_TYPE),
          Source.of(wrappedBuffer(out.toByteArray())));
    };
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

  @Test
  @DisplayName("$http 6")
  void http6() {
    runHttpTest("http6");
  }

  @Test
  @DisplayName("$http 7")
  void http7() {
    runHttpTest("http7");
  }

  @Test
  @DisplayName("$http 8")
  void http8() {
    runHttpTest("http8");
  }

  @Test
  @DisplayName("$http 9")
  void http9() {
    runHttpTest("http9");
  }

  private void runHttpTest(final String name) {
    assertEquals(
        output(name),
        runTest(
            read(resource(name, "pipeline.json")),
            objects(read(resource(name, "input.json"))).toList()));
  }

  @BeforeEach
  void startServer() {
    server =
        new HttpServer(
            9000,
            when(request -> request.method().name().equals("POST"), accumulate(postHandler()))
                .orElse(accumulate(resourceHandler(path -> "application/json"))));
    server.run();
  }

  @AfterEach
  void stopServer() {
    server.close();
  }
}
