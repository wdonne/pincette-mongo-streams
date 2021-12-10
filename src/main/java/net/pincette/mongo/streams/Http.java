package net.pincette.mongo.streams;

import static io.netty.handler.ssl.SslContextBuilder.forClient;
import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.ofNullable;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createParser;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.json.filter.Util.stream;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.StreamUtil.iterable;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static org.asynchttpclient.Dsl.asyncHttpClient;

import io.netty.handler.ssl.SslContext;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.net.ssl.KeyManagerFactory;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

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

  private static JsonObject addError(final JsonObject json, final Response response) {
    return createObjectBuilder(json)
        .add(
            HTTP_ERROR,
            create(JsonUtil::createObjectBuilder)
                .update(b -> b.add(STATUS_CODE, response.getStatusCode()))
                .updateIf(() -> getBody(response), (b, v) -> b.add(BODY, v))
                .build())
        .build();
  }

  private static JsonObject addResponseBody(
      final JsonObject json, final String as, final JsonValue body) {
    return as != null ? createObjectBuilder(json).add(as, body).build() : json;
  }

  private static Request createRequest(
      final JsonObject value,
      final Function<JsonObject, JsonValue> url,
      final Function<JsonObject, JsonValue> method,
      final Function<JsonObject, JsonValue> headers,
      final Function<JsonObject, JsonValue> body) {
    final String m = stringValue(method.apply(value)).orElse(null);
    final String u = stringValue(url.apply(value)).orElse(null);

    return u != null && m != null
        ? create(RequestBuilder::new)
            .update(b -> b.setUrl(u))
            .update(b -> b.setMethod(m))
            .updateIf(
                () ->
                    ofNullable(headers)
                        .map(h -> h.apply(value))
                        .filter(JsonUtil::isObject)
                        .map(JsonValue::asJsonObject),
                Http::setHeaders)
            .updateIf(
                () -> ofNullable(body).map(b -> b.apply(value)).filter(JsonUtil::isStructure),
                Http::setBody)
            .build()
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

  private static Response execute(
      final AsyncHttpClient client, final Request request, final Context context) {
    return tryForever(() -> client.executeRequest(request).toCompletableFuture(), "$http", context);
  }

  private static Function<JsonObject, Response> execute(
      final AsyncHttpClient client,
      final Function<JsonObject, JsonValue> url,
      final Function<JsonObject, JsonValue> method,
      final Function<JsonObject, JsonValue> headers,
      final Function<JsonObject, JsonValue> body,
      final Context context) {
    return json -> execute(client, createRequest(json, url, method, headers, body), context);
  }

  private static Optional<JsonValue> getBody(final Response response) {
    return isJson(response)
        ? from(response.getResponseBody()).map(JsonValue.class::cast)
        : Optional.ofNullable(response.getContentType())
            .filter(type -> type.startsWith("text/"))
            .map(type -> createValue(response.getResponseBody(UTF_8)));
  }

  private static AsyncHttpClient getClient(final JsonObject sslContext) {
    return asyncHttpClient(getConfig(sslContext));
  }

  private static AsyncHttpClientConfig getConfig(final JsonObject sslContext) {
    return create(DefaultAsyncHttpClientConfig.Builder::new)
        .update(b -> b.setFollowRedirect(true))
        .updateIf(b -> sslContext != null, b -> b.setSslContext(createSslContext(sslContext)))
        .build()
        .build();
  }

  private static KeyManagerFactory getKeyManagerFactory(
      final KeyStore keyStore, final String password) {
    return tryToGetRethrow(
            () -> KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()))
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

  private static Stream<JsonObject> getUnwoundBody(final Response response) {
    return stream(createParser(response.getResponseBodyAsStream()))
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject);
  }

  private static boolean isJson(final Response response) {
    return Optional.ofNullable(response.getContentType())
        .filter(type -> type.startsWith("application/json"))
        .isPresent();
  }

  private static BiFunction<JsonObject, Response, Iterable<JsonObject>> multiple(final String as) {
    return (json, response) ->
        ok(response)
            ? iterable(getUnwoundBody(response).map(body -> addResponseBody(json, as, body)))
            : list(addError(json, response));
  }

  private static boolean ok(final Response response) {
    return response.getStatusCode() < 300;
  }

  private static RequestBuilder setBody(final RequestBuilder builder, final JsonValue body) {
    final byte[] b = string(body).getBytes(UTF_8);

    return builder
        .addHeader("Content-Type", "application/json")
        .addHeader("Content-Length", valueOf(b.length))
        .setBody(b);
  }

  private static RequestBuilder setHeaders(final RequestBuilder builder, final JsonObject headers) {
    return toNative(headers).entrySet().stream()
        .reduce(builder, (b, e) -> b.addHeader(e.getKey(), e.getValue()), (b1, b2) -> b1);
  }

  private static BiFunction<JsonObject, Response, JsonObject> single(final String as) {
    return (json, response) ->
        ok(response)
            ? getBody(response).map(body -> addResponseBody(json, as, body)).orElse(json)
            : addError(json, response);
  }

  static KStream<String, JsonObject> stage(
      final KStream<String, JsonObject> stream, final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();

    must(expr.containsKey(METHOD) && expr.containsKey(URL));

    final String as = expr.getString(AS, null);
    final Function<JsonObject, Response> execute =
        execute(
            getClient(expr.getJsonObject(SSL_CONTEXT)),
            function(expr.getValue("/" + URL), context.features),
            function(expr.getValue("/" + METHOD), context.features),
            getValue(expr, "/" + HEADERS).map(h -> function(h, context.features)).orElse(null),
            getValue(expr, "/" + BODY).map(b -> function(b, context.features)).orElse(null),
            context);
    final ValueMapper<JsonObject, Iterable<JsonObject>> multiple =
        json -> multiple(as).apply(json, execute.apply(json));
    final ValueMapper<JsonObject, JsonObject> single =
        json -> single(as).apply(json, execute.apply(json));

    return as != null && expr.getBoolean(UNWIND, false)
        ? stream.flatMapValues(multiple)
        : stream.mapValues(single);
  }
}
