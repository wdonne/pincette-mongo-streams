package net.pincette.mongo.streams;

import static java.lang.Math.round;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.stream.Collectors.toList;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.util.StreamUtil.rangeInclusive;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import javax.json.JsonObject;
import net.pincette.rs.streams.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestThrottle extends Base {
  private static boolean withinPercent(
      final int total, final int max, final long measured, final int percent) {
    final int exact = total / max;
    final long deviation = round(exact * percent / 100.0);

    return measured >= exact - deviation && measured <= exact + deviation;
  }

  @Test
  @DisplayName("$throttle")
  void throttle() {
    final int max = 10;
    final int total = 100;
    final List<JsonObject> messages =
        rangeInclusive(0, total).map(v -> o(f("value", v(v)))).collect(toList());
    final Instant start = now();
    final List<Message<String, JsonObject>> result =
        runTest(a(o(f("$throttle", o(f("maxPerSecond", v(max)))))), messages);

    assertTrue(withinPercent(total, max, between(start, now()).getSeconds(), 5));
    assertEquals(messages, result.stream().map(m -> m.value).collect(toList()));
  }
}
