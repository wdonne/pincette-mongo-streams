package net.pincette.mongo.streams;

import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Mapper.map;
import static net.pincette.util.Util.must;

import java.util.concurrent.Flow.Processor;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.rs.streams.Message;

/**
 * The <code>$count</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Count {
  private static final String ID = "_id";
  private static final String SUM = "$sum";

  private Count() {}

  static Processor<Message<String, JsonObject>, Message<String, JsonObject>> stage(
      final JsonValue expression, final Context context) {
    must(isString(expression));

    final String field = asString(expression).getString();

    return box(
        Group.stage(
            createObjectBuilder()
                .add(ID, NULL)
                .add(field, createObjectBuilder().add(SUM, 1))
                .build(),
            context),
        map(m -> m.withValue(createObjectBuilder(m.value).remove(ID).build())));
  }
}
