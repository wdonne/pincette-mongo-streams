# JSON Streaming With Mongo Streams

This small library allows you to write JSON data streaming pipelines using [MongoDB Aggregation Pipeline Stages](https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/). It works with 
[Kafka Streams](https://kafka.apache.org/documentation/streams/) and the MongoDB database. Its only class has a ```create``` method to which you pass a Kafka stream. It will return another Kafka stream. Only the pipeline stages that are relevant for infinite streams are implemented. There are also a few extension stages to trace and probe your data pipelines. Finally, it includes a 
[JSLT](https://github.com/schibsted/jslt) stage, which is useful for transformations that would be too complicated with the MongoDB language alone.

The details are documented in the [API documentation](https://www.javadoc.io/doc/net.pincette/pincette-mongo-streams/latest/index.html).