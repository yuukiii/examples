/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.examples.streams.queryablestate;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Arrays;
import java.util.Properties;

public class QueryableStateExample {

  static final String TEXT_LINES_TOPIC = "TextLinesTopic";

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage ... port");
    }
    final int port = Integer.valueOf(args[0]);

    Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    streamsConfiguration.put(StreamsConfig.USER_ENDPOINT_CONFIG, "localhost:" + port);


    KafkaStreams streams = createStreams(streamsConfiguration);
    // Now that we have finished the definition of the processing topology we can actually run
    // it via `start()`.  The Streams application as a whole can be launched just like any
    // normal Java application that has a `main()` method.
    streams.start();

    // Start the Restful proxy for servicing remote access to state stores
    startRestProxy(streams, port);

  }


  static QueryableStateProxy startRestProxy(final KafkaStreams streams, final int port)
      throws Exception {
    final QueryableStateProxy queryableStateProxy = new QueryableStateProxy(streams);
    queryableStateProxy.start(port);
    return queryableStateProxy;
  }

  static KafkaStreams createStreams(final Properties streamsConfiguration) {
    final Serde<String> stringSerde = Serdes.String();
    KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String>
        textLines = builder.stream(stringSerde, stringSerde, TEXT_LINES_TOPIC);

    final KGroupedStream<String, String> groupedByWord = textLines
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        .groupBy((key, word) -> word, stringSerde, stringSerde);

    // Create a State Store for with the all time word count
    groupedByWord.count("word-count");

    // Create a Windowed State Store that contains the word count for every
    // 1 minute
    groupedByWord.count(TimeWindows.of(60000),"windowed-word-count");

    return new KafkaStreams(builder, streamsConfiguration);
  }

}