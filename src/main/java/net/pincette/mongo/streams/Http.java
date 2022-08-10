package net.pincette.mongo.streams;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.ssl.SslContextBuilder.forClient;
import static java.nio.ByteBuffer.wrap;
import static java.util.Optional.ofNullable;
import static javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Pipeline.HTTP;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.netty.http.HttpClient.hasBody;
import static net.pincette.netty.http.Util.setBody;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Flatten.flatMap;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.rs.Util.discard;
import static net.pincette.rs.json.Util.parseJson;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.SslContext;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.net.ssl.KeyManagerFactory;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.netty.http.HttpClient;
import net.pincette.rs.Source;
import net.pincette.rs.streams.Message;

/**
 * The $http operator.
 *
 * @author Werner Donn\u00e9
 */
class Http {
  private static final String AS = "as";
  private static final String BODY = "body";
  private static final String HEADERS = "headers";
  private static final String HTTP_ERROR = "httpError";
  private static final String KEY_STORE = "keyStore";
  private static final String METHOD = "method";
  private static final String PASSWORD = "password";
  private static final String SSL_CONTEXT = "sslContext";
  private static final String STATUS_CODE = "statusCode";
  private static final String UNWIND = "unwind";
  private static final String URL = "url";

  private Http() {}

  private static JsonObject addBody(
      final JsonObject json, final JsonValue value, final String field) {
    return createObjectBuilder(json).add(field, value).build();
  }

  private static Publisher<Message<String, JsonObject>> addError(
      final Message<String, JsonObject> message,
      final HttpResponse response,
      final Publisher<ByteBuf> responseBody) {
    final JsonObjectBuilder error =
        createObjectBuilder().add(STATUS_CODE, response.status().code());
    final JsonObjectBuilder newValue = createObjectBuilder(message.value);

    return getResponseBody(response, responseBody)
        .map(
            publisher ->
                with(Source.of(message))
                    .mapAsync(m -> reduceResponseBody(publisher).thenApply(body -> pair(m, body)))
                    .map(
                        pair ->
                            pair.first.withValue(
                                newValue.add(HTTP_ERROR, error.add(BODY, pair.second)).build()))
                    .get())
        .orElseGet(() -> Source.of(message.withValue(newValue.add(HTTP_ERROR, error).build())));
  }

  private static FullHttpRequest createRequest(
      final JsonObject value,
      final Function<JsonObject, JsonValue> url,
      final Function<JsonObject, JsonValue> method,
      final Function<JsonObject, JsonValue> headers,
      final Function<JsonObject, JsonValue> body) {
    final String m = stringValue(method.apply(value)).orElse(null);
    final String u = stringValue(url.apply(value)).orElse(null);

    return u != null && m != null
        ? create(() -> new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.valueOf(m), u))
            .updateIf(
                () ->
                    ofNullable(headers)
                        .map(h -> h.apply(value))
                        .filter(JsonUtil::isObject)
                        .map(JsonValue::asJsonObject),
                Http::setHeaders)
            .updateIf(
                () -> ofNullable(body).map(b -> b.apply(value)).filter(JsonUtil::isStructure),
                Http::setJsonBody)
            .build()
        : null;
  }

  private static SslContext createSslContext(final JsonObject sslContext) {
    return tryToGetRethrow(
            () ->
                forClient()
                    .keyManager(
                        getKeyManagerFactory(
                            getKeyStore(
                                sslContext.getString(KEY_STORE), sslContext.getString(PASSWORD)),
                            sslContext.getString(PASSWORD)))
                    .build())
        .orElse(null);
  }

  private static CompletionStage<HttpResponse> execute(
      final HttpClient client,
      final FullHttpRequest request,
      final Subscriber<? super ByteBuf> responseBody,
      final Context context) {
    return tryForever(() -> client.request(request, responseBody), HTTP, context);
  }

  private static BiFunction<JsonObject, Subscriber<ByteBuf>, CompletionStage<HttpResponse>> execute(
      final HttpClient client,
      final Function<JsonObject, JsonValue> url,
      final Function<JsonObject, JsonValue> method,
      final Function<JsonObject, JsonValue> headers,
      final Function<JsonObject, JsonValue> requestBody,
      final Context context) {
    return (json, responseBody) ->
        execute(
            client, createRequest(json, url, method, headers, requestBody), responseBody, context);
  }

  private static HttpClient getClient(final JsonObject sslContext) {
    return create(HttpClient::new)
        .update(b -> b.withFollowRedirects(true))
        .updateIf(b -> sslContext != null, b -> b.withSslContext(createSslContext(sslContext)))
        .build();
  }

  private static KeyManagerFactory getKeyManagerFactory(
      final KeyStore keyStore, final String password) {
    return tryToGetRethrow(() -> KeyManagerFactory.getInstance(getDefaultAlgorithm()))
        .map(
            factory ->
                SideEffect.<KeyManagerFactory>run(
                        () -> tryToDoRethrow(() -> factory.init(keyStore, password.toCharArray())))
                    .andThenGet(() -> factory))
        .orElse(null);
  }

  private static KeyStore getKeyStore(final String keyStore, final String password) {
    return tryToGetRethrow(() -> KeyStore.getInstance("pkcs12"))
        .map(
            store ->
                SideEffect.<KeyStore>run(
                        () ->
                            tryToDoRethrow(
                                () ->
                                    store.load(
                                        new FileInputStream(keyStore), password.toCharArray())))
                    .andThenGet(() -> store))
        .orElse(null);
  }

  private static Optional<Publisher<JsonObject>> getResponseBody(
      final HttpResponse response, final Publisher<ByteBuf> responseBody) {
    final Optional<Publisher<JsonObject>> result =
        Optional.of(response)
            .filter(HttpClient::hasBody)
            .filter(Http::isJson)
            .map(r -> responseBodyPublisher(responseBody));

    if (hasBody(response) && result.isEmpty()) {
      discard(responseBody);
    }

    return result;
  }

  private static boolean isJson(final HttpResponse response) {
    return Optional.ofNullable(response.headers().get(CONTENT_TYPE))
        .filter(type -> type.startsWith("application/json"))
        .isPresent();
  }

  private static boolean ok(final HttpResponse response) {
    return response.status().code() < 300;
  }

  private static CompletionStage<JsonStructure> reduceResponseBody(
      final Publisher<JsonObject> responseBody) {
    return reduce(responseBody, JsonUtil::createArrayBuilder, JsonArrayBuilder::add)
        .thenApply(JsonArrayBuilder::build)
        .thenApply(array -> array.size() == 1 ? array.get(0).asJsonObject() : array);
  }

  private static Publisher<JsonObject> responseBodyPublisher(
      final Publisher<ByteBuf> responseBody) {
    return with(responseBody)
        .map(Http::toNioBuffer)
        .map(parseJson())
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .get();
  }

  private static void setJsonBody(final FullHttpRequest request, final JsonValue body) {
    setBody(request, "application/json", string(body));
  }

  private static void setHeaders(final FullHttpRequest request, final JsonObject headers) {
    toNative(headers)
        .forEach(
            (k, v) -> {
              if (v instanceof List) {
                request.headers().add(k, (List<?>) v);
              } else {
                request.headers().add(k, v);
              }
            });
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();

    must(expr.containsKey(METHOD) && expr.containsKey(URL));

    final String as = expr.getString(AS, null);
    final HttpClient client = getClient(expr.getJsonObject(SSL_CONTEXT));
    final BiFunction<JsonObject, Subscriber<ByteBuf>, CompletionStage<HttpResponse>> execute =
        execute(
            client,
            function(expr.getValue("/" + URL), context.features),
            function(expr.getValue("/" + METHOD), context.features),
            getValue(expr, "/" + HEADERS).map(h -> function(h, context.features)).orElse(null),
            getValue(expr, "/" + BODY).map(b -> function(b, context.features)).orElse(null),
            context);

    return pipe(map(WithNewResponseBody::withNewResponseBody))
        .then(
            mapAsync(
                message ->
                    execute
                        .apply(message.message.value, message.responseBody)
                        .thenApply(response -> pair(message, response))))
        .then(
            flatMap(
                pair ->
                    transform(
                        pair.first.message,
                        pair.second,
                        pair.first.responseBody,
                        as,
                        expr.getBoolean(UNWIND, false))));
  }

  private static ByteBuffer toNioBuffer(final ByteBuf buffer) {
    final byte[] bytes = new byte[buffer.readableBytes()];

    buffer.readBytes(bytes);

    return wrap(bytes);
  }

  private static Publisher<Message<String, JsonObject>> transform(
      final Message<String, JsonObject> message,
      final HttpResponse response,
      final Publisher<ByteBuf> responseBody,
      final String as,
      final boolean unwind) {
    final Supplier<Publisher<Message<String, JsonObject>>> tryWithBody =
        () ->
            as != null
                ? withResponseBody(message, response, responseBody, as, unwind)
                : withoutResponseBody(message, responseBody);

    return ok(response) ? tryWithBody.get() : addError(message, response, responseBody);
  }

  private static Publisher<Message<String, JsonObject>> unwindResponseBody(
      final Message<String, JsonObject> message,
      final Publisher<JsonObject> responseBody,
      final String as) {
    return with(responseBody)
        .map(body -> message.withValue(addBody(message.value, body, as)))
        .get();
  }

  private static Publisher<Message<String, JsonObject>> withoutResponseBody(
      final Message<String, JsonObject> message, final Publisher<ByteBuf> responseBody) {
    discard(responseBody);

    return Source.of(message);
  }

  private static Publisher<Message<String, JsonObject>> withReducedResponseBody(
      final Message<String, JsonObject> message,
      final Publisher<JsonObject> responseBody,
      final String as) {
    return with(Source.of(message))
        .mapAsync(
            m ->
                reduceResponseBody(responseBody)
                    .thenApply(body -> m.withValue(addBody(m.value, body, as))))
        .get();
  }

  private static Publisher<Message<String, JsonObject>> withResponseBody(
      final Message<String, JsonObject> message,
      final HttpResponse response,
      final Publisher<ByteBuf> responseBody,
      final String as,
      final boolean unwind) {
    return getResponseBody(response, responseBody)
        .map(
            body ->
                unwind
                    ? unwindResponseBody(message, body, as)
                    : withReducedResponseBody(message, body, as))
        .orElseGet(() -> Source.of(message));
  }

  private static class WithNewResponseBody {
    private final Message<String, JsonObject> message;
    private final Processor<ByteBuf, ByteBuf> responseBody;

    private WithNewResponseBody(
        final Message<String, JsonObject> message, final Processor<ByteBuf, ByteBuf> responseBody) {
      this.message = message;
      this.responseBody = responseBody;
    }

    static WithNewResponseBody withNewResponseBody(final Message<String, JsonObject> message) {
      return new WithNewResponseBody(message, passThrough());
    }
  }
}
