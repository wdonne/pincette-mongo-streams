package net.pincette.mongo.streams;

import static java.util.UUID.randomUUID;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Per.per;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Util.must;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

class Per {
  private static final String AMOUNT = "amount";
  private static final String AS = "as";
  private static final String TIMEOUT = "timeout";

  private Per() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression) {
    must(isObject(expression));

    final JsonObject expr = expression.asJsonObject();

    must(expr.containsKey(AMOUNT) && expr.containsKey(AS));

    final String as = expr.getString(AS);
    final Duration timeout =
        Optional.of(expr.getInt(TIMEOUT, -1))
            .filter(t -> t != -1)
            .map(Duration::ofMillis)
            .orElse(null);

    return box(
        per(expr.getInt(AMOUNT), timeout),
        map(
            list ->
                message(
                    randomUUID().toString(),
                    createObjectBuilder().add(as, from(list.stream().map(m -> m.value))).build())));
  }
}
