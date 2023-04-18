package net.pincette.mongo.streams;

import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers.ofPublisher;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static javax.json.JsonValue.NULL;
import static javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Pipeline.HTTP;
import static net.pincette.mongo.streams.Util.RETRY;
import static net.pincette.mongo.streams.Util.exceptionLogger;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Flatten.flatMap;
import static net.pincette.rs.FlattenList.flattenList;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.rs.Util.completablePublisher;
import static net.pincette.rs.Util.discard;
import static net.pincette.rs.Util.lines;
import static net.pincette.rs.Util.retryPublisher;
import static net.pincette.rs.json.Util.parseJson;
import static net.pincette.util.ImmutableBuilder.create;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import java.io.FileInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.rs.Source;
import net.pincette.rs.streams.Message;
import net.pincette.util.State;

/**
 * The $http operator.
 *
 * @author Werner Donné
 */
class Http {
  private static final String AS = "as";
  private static final String BODY = "body";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String HEADERS = "headers";
  private static final String HTTP_ERROR = "httpError";
  private static final String JSON = "application/json";
  private static final String KEY_STORE = "keyStore";
  private static final String METHOD = "method";
  private static final String PASSWORD = "password";
  private static final String SSL_CONTEXT = "sslContext";
  private static final String STATUS_CODE = "statusCode";
  private static final String TEXT = "text/plain";
  private static final String UNWIND = "unwind";
  private static final String URL = "url";

  private Http() {}

  private static JsonObject addBadRequest(final JsonObject value) {
    return addError(value, 400, null);
  }

  private static JsonObject addBody(
      final JsonObject json, final JsonValue value, final String field) {
    return createObjectBuilder(json).add(field, value).build();
  }

  private static Publisher<Message<String, JsonObject>> addError(
      final Message<String, JsonObject> message,
      final HttpResponse<Publisher<List<ByteBuffer>>> response) {
    return with(Source.of(message))
        .mapAsync(
            m ->
                reducedResponseBody(response)
                    .thenApply(body -> m.withValue(addError(m.value, response.statusCode(), body))))
        .get();
  }

  private static JsonObject addError(
      final JsonObject value, final int statusCode, final JsonValue body) {
    return createObjectBuilder(value)
        .add(
            HTTP_ERROR,
            create(JsonUtil::createObjectBuilder)
                .update(b -> b.add(STATUS_CODE, statusCode))
                .updateIf(() -> ofNullable(body), (b, v) -> b.add(BODY, v))
                .build())
        .build();
  }

  private static CompletionStage<JsonObject> addResponseBody(
      final JsonObject value,
      final HttpResponse<Publisher<List<ByteBuffer>>> response,
      final String as) {
    return reducedResponseBody(response)
        .thenApply(
            b ->
                ok(response)
                    ? addResponseBody(value, b, as)
                    : addError(value, response.statusCode(), b));
  }

  private static JsonObject addResponseBody(
      final JsonObject value, final JsonValue body, final String as) {
    return as != null ? createObjectBuilder(value).add(as, body).build() : value;
  }

  private static Optional<JsonValue> body(
      final JsonObject value, final Function<JsonObject, JsonValue> body) {
    return ofNullable(body).map(b -> b.apply(value)).filter(JsonUtil::isStructure);
  }

  private static Publisher<ByteBuffer> body(
      final HttpResponse<Publisher<List<ByteBuffer>>> response) {
    return with(response.body()).map(flattenList()).get();
  }

  private static HttpRequest createRequest(final RequestInput input) {
    return create(
            () ->
                HttpRequest.newBuilder(input.uri)
                    .method(
                        input.method,
                        ofNullable(input.body)
                            .map(BodyPublishers::ofString)
                            .orElseGet(BodyPublishers::noBody)))
        .updateIf(() -> ofNullable(input.headers), Http::setHeaders)
        .build()
        .build();
  }

  private static Optional<SSLContext> createSslContext(final JsonObject sslContext) {
    final String password = sslContext.getString(PASSWORD);

    return tryToGetRethrow(() -> SSLContext.getInstance("TLSv1.3"))
        .flatMap(
            context ->
                getKeyStore(sslContext.getString(KEY_STORE), password)
                    .flatMap(store -> getKeyManagerFactory(store, password))
                    .flatMap(Http::getKeyManagers)
                    .map(managers -> pair(context, managers)))
        .map(
            pair ->
                SideEffect.<SSLContext>run(
                        () -> tryToDoRethrow(() -> pair.first.init(pair.second, null, null)))
                    .andThenGet(() -> pair.first));
  }

  private static Optional<CompletionStage<HttpResponse<Publisher<List<ByteBuffer>>>>> execute(
      final HttpClient client,
      final Supplier<Optional<HttpRequest>> request,
      final Context context) {
    final State<String> message = new State<>();

    return request
        .get()
        .map(
            r ->
                tryForever(
                    () ->
                        SideEffect.<CompletionStage<HttpResponse<Publisher<List<ByteBuffer>>>>>run(
                                () -> message.set(r.method() + " of " + r.uri()))
                            .andThenGet(() -> client.sendAsync(r, ofPublisher())),
                    HTTP,
                    message::get,
                    context));
  }

  private static Function<
          JsonObject, Optional<CompletionStage<HttpResponse<Publisher<List<ByteBuffer>>>>>>
      execute(
          final HttpClient client,
          final Function<JsonObject, JsonValue> url,
          final Function<JsonObject, JsonValue> method,
          final Function<JsonObject, JsonValue> headers,
          final Function<JsonObject, JsonValue> requestBody,
          final Context context) {
    return json ->
        execute(
            client,
            () -> requestInput(json, url, method, headers, requestBody).map(Http::createRequest),
            context);
  }

  private static HttpClient getClient(final JsonObject expression) {
    final HttpClient.Builder builder =
        newBuilder().version(Version.HTTP_1_1).followRedirects(Redirect.NORMAL);

    return ofNullable(expression.getJsonObject(SSL_CONTEXT))
        .flatMap(Http::createSslContext)
        .map(builder::sslContext)
        .orElse(builder)
        .build();
  }

  private static Optional<KeyManager[]> getKeyManagers(final KeyManagerFactory factory) {
    return Optional.of(factory.getKeyManagers()).filter(managers -> managers.length > 0);
  }

  private static Optional<KeyManagerFactory> getKeyManagerFactory(
      final KeyStore keyStore, final String password) {
    return tryToGetRethrow(() -> KeyManagerFactory.getInstance(getDefaultAlgorithm()))
        .map(
            factory ->
                SideEffect.<KeyManagerFactory>run(
                        () -> tryToDoRethrow(() -> factory.init(keyStore, password.toCharArray())))
                    .andThenGet(() -> factory));
  }

  private static Optional<KeyStore> getKeyStore(final String keyStore, final String password) {
    return tryToGetRethrow(() -> KeyStore.getInstance("pkcs12"))
        .map(
            store ->
                SideEffect.<KeyStore>run(
                        () ->
                            tryToDoRethrow(
                                () ->
                                    store.load(
                                        new FileInputStream(keyStore), password.toCharArray())))
                    .andThenGet(() -> store));
  }

  private static Optional<Publisher<JsonObject>> getResponseBody(
      final HttpResponse<Publisher<List<ByteBuffer>>> response) {
    final Optional<Publisher<JsonObject>> result =
        Optional.of(response).filter(Http::isJson).map(r -> responseBodyPublisher(body(response)));

    if (result.isEmpty()) {
      discard(body(response));
    }

    return result;
  }

  private static Optional<JsonObject> headers(
      final JsonObject value, final Function<JsonObject, JsonValue> headers) {
    return ofNullable(headers)
        .map(h -> h.apply(value))
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject);
  }

  private static <T> boolean isJson(final HttpResponse<T> response) {
    return isType(response, JSON);
  }

  private static <T> boolean isText(final HttpResponse<T> response) {
    return isType(response, TEXT);
  }

  private static <T> boolean isType(final HttpResponse<T> response, final String contentType) {
    return Optional.ofNullable(response.headers())
        .flatMap(h -> h.firstValue(CONTENT_TYPE))
        .filter(type -> type.startsWith(contentType))
        .isPresent();
  }

  private static <T> boolean ok(final HttpResponse<T> response) {
    return response.statusCode() < 300;
  }

  private static Consumer<Throwable> onException(final Context context) {
    return e -> exceptionLogger(e, HTTP, e::getMessage, context);
  }

  private static CompletionStage<JsonValue> reducedResponseBody(
      final HttpResponse<Publisher<List<ByteBuffer>>> response) {
    return Optional.of(response)
        .filter(Http::isJson)
        .map(r -> reduceResponseBodyJson(responseBodyPublisher(body(response))))
        .orElseGet(
            () ->
                isText(response)
                    ? reduceResponseBodyText(body(response))
                    : completedFuture(withoutResponseBody(body(response))));
  }

  private static CompletionStage<JsonValue> reduceResponseBodyJson(
      final Publisher<JsonObject> responseBody) {
    return reduce(responseBody, JsonUtil::createArrayBuilder, JsonArrayBuilder::add)
        .thenApply(JsonArrayBuilder::build)
        .thenApply(array -> array.size() == 1 ? array.get(0).asJsonObject() : array);
  }

  private static CompletionStage<JsonValue> reduceResponseBodyText(
      final Publisher<ByteBuffer> responseBody) {
    return reduce(with(responseBody).map(lines()).get(), StringBuilder::new, StringBuilder::append)
        .thenApply(StringBuilder::toString)
        .thenApply(JsonUtil::createValue);
  }

  private static Optional<RequestInput> requestInput(
      final JsonObject value,
      final Function<JsonObject, JsonValue> url,
      final Function<JsonObject, JsonValue> method,
      final Function<JsonObject, JsonValue> headers,
      final Function<JsonObject, JsonValue> body) {
    final URI uri =
        stringValue(url.apply(value)).flatMap(u -> tryToGetSilent(() -> new URI(u))).orElse(null);
    final String m = stringValue(method.apply(value)).orElse(null);
    final String b = body(value, body).map(JsonUtil::string).orElse(null);
    final JsonObject h =
        body != null
            ? createObjectBuilder(headers(value, headers).orElseGet(JsonUtil::emptyObject))
                .add(CONTENT_TYPE, JSON)
                .build()
            : headers(value, headers).orElse(null);

    return ofNullable(uri).filter(u -> m != null).map(u -> new RequestInput(u, m, h, b));
  }

  private static Publisher<JsonObject> responseBodyPublisher(
      final Publisher<ByteBuffer> responseBody) {
    return with(responseBody)
        .map(parseJson())
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .get();
  }

  private static Function<Message<String, JsonObject>, CompletionStage<Message<String, JsonObject>>>
      retryExecute(
          final Function<
                  JsonObject, Optional<CompletionStage<HttpResponse<Publisher<List<ByteBuffer>>>>>>
              execute,
          final String as,
          final Context context) {
    return message ->
        tryForever(
            () ->
                execute
                    .apply(message.value)
                    .map(
                        response ->
                            response
                                .thenComposeAsync(r -> addResponseBody(message.value, r, as))
                                .thenApply(message::withValue))
                    .orElseGet(
                        () -> completedFuture(message.withValue(addBadRequest(message.value)))),
            HTTP,
            () -> null,
            context);
  }

  private static Function<Message<String, JsonObject>, Publisher<Message<String, JsonObject>>>
      retryExecuteUnwind(
          final Function<
                  JsonObject, Optional<CompletionStage<HttpResponse<Publisher<List<ByteBuffer>>>>>>
              execute,
          final String as,
          final Context context) {
    return message ->
        retryPublisher(
            () ->
                completablePublisher(
                    () ->
                        execute
                            .apply(message.value)
                            .map(response -> response.thenApply(r -> transform(message, r, as)))
                            .orElseGet(
                                () ->
                                    completedFuture(
                                        Source.of(
                                            message.withValue(addBadRequest(message.value)))))),
            RETRY,
            onException(context));
  }

  private static HttpRequest.Builder setHeaders(
      final HttpRequest.Builder builder, final JsonObject headers) {
    return builder.headers(
        headers.entrySet().stream()
            .flatMap(
                e ->
                    Optional.of(e.getValue())
                        .filter(JsonUtil::isArray)
                        .map(JsonValue::asJsonArray)
                        .map(JsonArray::stream)
                        .map(s -> s.flatMap(v -> Stream.of(e.getKey(), toNative(v).toString())))
                        .orElseGet(() -> Stream.of(e.getKey(), toNative(e.getValue()).toString())))
            .toArray(String[]::new));
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();

    must(expr.containsKey(METHOD) && expr.containsKey(URL));

    final String as = expr.getString(AS, null);
    final HttpClient client = getClient(expr);
    final Function<JsonObject, Optional<CompletionStage<HttpResponse<Publisher<List<ByteBuffer>>>>>>
        execute =
            execute(
                client,
                function(expr.getValue("/" + URL), context.features),
                function(expr.getValue("/" + METHOD), context.features),
                getValue(expr, "/" + HEADERS).map(h -> function(h, context.features)).orElse(null),
                getValue(expr, "/" + BODY).map(b -> function(b, context.features)).orElse(null),
                context);

    return expr.getBoolean(UNWIND, false) && as != null
        ? flatMap(retryExecuteUnwind(execute, as, context))
        : mapAsyncSequential(retryExecute(execute, as, context));
  }

  private static Publisher<Message<String, JsonObject>> transform(
      final Message<String, JsonObject> message,
      final HttpResponse<Publisher<List<ByteBuffer>>> response,
      final String as) {
    final Supplier<Publisher<Message<String, JsonObject>>> tryWithBody =
        () -> withResponseBody(message, response, as);

    return ok(response) ? tryWithBody.get() : addError(message, response);
  }

  private static Publisher<Message<String, JsonObject>> unwindResponseBody(
      final Message<String, JsonObject> message,
      final Publisher<JsonObject> responseBody,
      final String as) {
    return with(responseBody)
        .map(body -> message.withValue(addBody(message.value, body, as)))
        .get();
  }

  private static JsonValue withoutResponseBody(final Publisher<ByteBuffer> responseBody) {
    discard(responseBody);

    return NULL;
  }

  private static Publisher<Message<String, JsonObject>> withResponseBody(
      final Message<String, JsonObject> message,
      final HttpResponse<Publisher<List<ByteBuffer>>> response,
      final String as) {
    return getResponseBody(response)
        .map(body -> unwindResponseBody(message, body, as))
        .orElseGet(() -> Source.of(message));
  }

  private static class RequestInput {
    private final String body;
    private final JsonObject headers;
    private final String method;
    private final URI uri;

    private RequestInput(
        final URI uri, final String method, final JsonObject headers, final String body) {
      this.uri = uri;
      this.method = method;
      this.headers = headers;
      this.body = body;
    }
  }
}
