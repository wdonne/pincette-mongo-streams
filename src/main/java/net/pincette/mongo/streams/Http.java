package net.pincette.mongo.streams;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.streams.Pipeline.HTTP;
import static net.pincette.mongo.streams.Util.RETRY;
import static net.pincette.mongo.streams.Util.exceptionLogger;
import static net.pincette.mongo.streams.Util.tryForever;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Flatten.flatMap;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.rs.Util.asValueAsync;
import static net.pincette.rs.Util.completablePublisher;
import static net.pincette.rs.Util.discard;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.Util.lines;
import static net.pincette.rs.Util.retryPublisher;
import static net.pincette.rs.json.Util.parseJson;
import static net.pincette.util.ImmutableBuilder.create;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static org.eclipse.jetty.http.HttpHeader.CONTENT_LENGTH;
import static org.eclipse.jetty.http.HttpHeader.CONTENT_TYPE;
import static org.eclipse.jetty.http.HttpHeader.TRANSFER_ENCODING;
import static org.eclipse.jetty.reactive.client.ReactiveRequest.newBuilder;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;
import static org.reactivestreams.FlowAdapters.toPublisher;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.rs.Source;
import net.pincette.rs.streams.Message;
import net.pincette.util.Pair;
import net.pincette.util.State;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.dynamic.HttpClientTransportDynamic;
import org.eclipse.jetty.client.util.StringRequestContent;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.reactive.client.ContentChunk;
import org.eclipse.jetty.reactive.client.ReactiveResponse;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory.Client;

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

  private static final Map<JsonValue, HttpClient> clients = new HashMap<>();

  private Http() {}

  private static JsonObject addBody(
      final JsonObject json, final JsonValue value, final String field) {
    return createObjectBuilder(json).add(field, value).build();
  }

  private static Publisher<Message<String, JsonObject>> addError(
      final Message<String, JsonObject> message,
      final Response response,
      final Publisher<ByteBuffer> responseBody) {
    return with(Source.of(message))
        .mapAsync(
            m ->
                reducedResponseBody(response, responseBody)
                    .thenApply(body -> m.withValue(addError(m.value, response.getStatus(), body))))
        .get();
  }

  private static JsonObject addError(
      final JsonObject value, final int statusCode, final JsonValue body) {
    return createObjectBuilder(value)
        .add(HTTP_ERROR, createObjectBuilder().add(STATUS_CODE, statusCode).add(BODY, body))
        .build();
  }

  private static CompletionStage<JsonObject> addResponseBody(
      final JsonObject value,
      final Response response,
      final Publisher<ByteBuffer> responseBody,
      final String as) {
    return reducedResponseBody(response, responseBody)
        .thenApply(
            b ->
                ok(response)
                    ? addResponseBody(value, b, as)
                    : addError(value, response.getStatus(), b));
  }

  private static JsonObject addResponseBody(
      final JsonObject value, final JsonValue body, final String as) {
    return as != null ? createObjectBuilder(value).add(as, body).build() : value;
  }

  private static Optional<Request.Content> body(
      final JsonObject value, final Function<JsonObject, JsonValue> body) {
    return ofNullable(body)
        .map(b -> b.apply(value))
        .filter(JsonUtil::isStructure)
        .map(v -> new StringRequestContent("application/json", string(v)));
  }

  private static ClientConnector clientConnector(final SslContextFactory.Client sslContextFactory) {
    final ClientConnector connector = new ClientConnector();

    connector.setSslContextFactory(sslContextFactory);

    return connector;
  }

  private static JsonValue clientKey(final JsonObject expression) {
    return ofNullable(expression.getJsonObject(SSL_CONTEXT))
        .map(JsonValue.class::cast)
        .orElse(NULL);
  }

  private static Optional<Request> createRequest(
      final HttpClient client,
      final JsonObject value,
      final Function<JsonObject, JsonValue> url,
      final Function<JsonObject, JsonValue> method,
      final Function<JsonObject, JsonValue> headers,
      final Function<JsonObject, JsonValue> body) {
    return stringValue(url.apply(value))
        .flatMap(u -> stringValue(method.apply(value)).map(m -> pair(u, m)))
        .map(
            pair ->
                create(() -> client.newRequest(pair.first).method(pair.second))
                    .updateIf(() -> headers(value, headers), Http::setHeaders)
                    .updateIf(() -> body(value, body), Request::body)
                    .build());
  }

  private static Optional<SslContextFactory.Client> createSslContextFactory(
      final JsonObject sslContext) {
    return getKeyStore(sslContext.getString(KEY_STORE), sslContext.getString(PASSWORD))
        .map(Http::createSslContextFactory);
  }

  private static SslContextFactory.Client createSslContextFactory(final KeyStore keyStore) {
    final Client client = new Client();

    client.setKeyStore(keyStore);

    return client;
  }

  private static CompletionStage<Pair<Response, Publisher<ByteBuffer>>> execute(
      final Supplier<Optional<Request>> request, final Context context) {
    final State<String> message = new State<>();

    return tryForever(
        () ->
            request
                .get()
                .map(
                    r ->
                        SideEffect.<CompletionStage<Pair<Response, Publisher<ByteBuffer>>>>run(
                                () -> message.set(r.getMethod() + " of " + r.getURI()))
                            .andThenGet(
                                () ->
                                    asValueAsync(
                                        toFlowPublisher(
                                            newBuilder(r).build().response(Http::response)))))
                .orElseGet(
                    () -> completedFuture(pair(new HttpResponse(null, null).status(400), empty()))),
        HTTP,
        message::get,
        context);
  }

  private static Function<JsonObject, CompletionStage<Pair<Response, Publisher<ByteBuffer>>>>
      execute(
          final HttpClient client,
          final Function<JsonObject, JsonValue> url,
          final Function<JsonObject, JsonValue> method,
          final Function<JsonObject, JsonValue> headers,
          final Function<JsonObject, JsonValue> requestBody,
          final Context context) {
    return json ->
        execute(() -> createRequest(client, json, url, method, headers, requestBody), context);
  }

  private static HttpClient getClient(final JsonObject expression) {
    return clients.computeIfAbsent(
        clientKey(expression),
        k -> {
          final HttpClient client =
              ofNullable(expression.getJsonObject(SSL_CONTEXT))
                  .flatMap(Http::createSslContextFactory)
                  .map(Http::clientConnector)
                  .map(connector -> new HttpClient(new HttpClientTransportDynamic(connector)))
                  .orElseGet(HttpClient::new);

          client.setFollowRedirects(true);
          tryToDoRethrow(client::start);

          return client;
        });
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
      final Response response, final Publisher<ByteBuffer> responseBody) {
    final Optional<Publisher<JsonObject>> result =
        Optional.of(response)
            .filter(Http::hasBody)
            .filter(Http::isJson)
            .map(r -> responseBodyPublisher(responseBody));

    if (hasBody(response) && result.isEmpty()) {
      discard(responseBody);
    }

    return result;
  }

  private static boolean hasBody(final Response response) {
    return ofNullable(response.getHeaders().getField(CONTENT_LENGTH))
            .map(HttpField::getIntValue)
            .filter(l -> l > 0)
            .isPresent()
        || ofNullable(response.getHeaders().getField(TRANSFER_ENCODING)).isPresent();
  }

  private static Optional<JsonObject> headers(
      final JsonObject value, final Function<JsonObject, JsonValue> headers) {
    return ofNullable(headers)
        .map(h -> h.apply(value))
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject);
  }

  private static boolean isJson(final Response response) {
    return isType(response, "application/json");
  }

  private static boolean isText(final Response response) {
    return isType(response, "text/plain");
  }

  private static boolean isType(final Response response, final String contentType) {
    return Optional.ofNullable(response.getHeaders().getField(CONTENT_TYPE))
        .map(HttpField::getValue)
        .filter(type -> type.startsWith(contentType))
        .isPresent();
  }

  private static boolean ok(final Response response) {
    return response.getStatus() < 300;
  }

  private static Consumer<Throwable> onException(final Context context) {
    return e -> exceptionLogger(e, HTTP, e::getMessage, context);
  }

  private static CompletionStage<JsonValue> reducedResponseBody(
      final Response response, final Publisher<ByteBuffer> body) {
    return Optional.of(response)
        .filter(Http::isJson)
        .map(r -> reduceResponseBodyJson(responseBodyPublisher(body)))
        .orElseGet(
            () ->
                isText(response)
                    ? reduceResponseBodyText(body)
                    : completedFuture(withoutResponseBody(body)));
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

  private static org.reactivestreams.Publisher<Pair<Response, Publisher<ByteBuffer>>> response(
      final ReactiveResponse response, final org.reactivestreams.Publisher<ContentChunk> body) {
    return toPublisher(
        Source.of(
            pair(response.getResponse(), with(toFlowPublisher(body)).map(c -> c.buffer).get())));
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
          final Function<JsonObject, CompletionStage<Pair<Response, Publisher<ByteBuffer>>>>
              execute,
          final String as,
          final Context context) {
    return message ->
        tryForever(
            () ->
                execute
                    .apply(message.value)
                    .thenComposeAsync(
                        pair -> addResponseBody(message.value, pair.first, pair.second, as))
                    .thenApply(message::withValue),
            HTTP,
            () -> null,
            context);
  }

  private static Function<Message<String, JsonObject>, Publisher<Message<String, JsonObject>>>
      retryExecuteUnwind(
          final Function<JsonObject, CompletionStage<Pair<Response, Publisher<ByteBuffer>>>>
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
                            .thenApply(pair -> transform(message, pair.first, pair.second, as))),
            RETRY,
            onException(context));
  }

  private static Request setHeaders(final Request request, final JsonObject headers) {
    return request.headers(
        h ->
            toNative(headers)
                .forEach(
                    (k, v) -> {
                      if (v instanceof List) {
                        h.put(k, (List<String>) v);
                      } else {
                        h.put(k, (String) v);
                      }
                    }));
  }

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();

    must(expr.containsKey(METHOD) && expr.containsKey(URL));

    final String as = expr.getString(AS, null);
    final HttpClient client = getClient(expr);
    final Function<JsonObject, CompletionStage<Pair<Response, Publisher<ByteBuffer>>>> execute =
        execute(
            client,
            function(expr.getValue("/" + URL), context.features),
            function(expr.getValue("/" + METHOD), context.features),
            getValue(expr, "/" + HEADERS).map(h -> function(h, context.features)).orElse(null),
            getValue(expr, "/" + BODY).map(b -> function(b, context.features)).orElse(null),
            context);

    return expr.getBoolean(UNWIND, false) && as != null
        ? flatMap(retryExecuteUnwind(execute, as, context))
        : mapAsync(retryExecute(execute, as, context));
  }

  private static Publisher<Message<String, JsonObject>> transform(
      final Message<String, JsonObject> message,
      final Response response,
      final Publisher<ByteBuffer> responseBody,
      final String as) {
    final Supplier<Publisher<Message<String, JsonObject>>> tryWithBody =
        () -> withResponseBody(message, response, responseBody, as);

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

  private static JsonValue withoutResponseBody(final Publisher<ByteBuffer> responseBody) {
    discard(responseBody);

    return NULL;
  }

  private static Publisher<Message<String, JsonObject>> withResponseBody(
      final Message<String, JsonObject> message,
      final Response response,
      final Publisher<ByteBuffer> responseBody,
      final String as) {
    return getResponseBody(response, responseBody)
        .map(body -> unwindResponseBody(message, body, as))
        .orElseGet(() -> Source.of(message));
  }
}
